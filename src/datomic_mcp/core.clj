(ns datomic-mcp.core
  (:require [clojure.data.json :as json]
            [datomic.api :as d]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:gen-class)
  (:import [io.modelcontextprotocol.server.transport StdioServerTransportProvider]
           [io.modelcontextprotocol.server McpServer McpServerFeatures
            McpServerFeatures$AsyncToolSpecification]
           [io.modelcontextprotocol.spec
            McpSchema$ServerCapabilities
            McpSchema$Tool
            McpSchema$CallToolResult
            McpSchema$TextContent]
           [reactor.core.publisher Mono]
           [com.fasterxml.jackson.databind ObjectMapper]))

;; Global connection and database URI
(def conn-atom (atom nil))
(def db-uri-atom (atom nil))

;; Default schema for common use cases
(def default-schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :person/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/friends
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident :person/parent
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/children
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident :company/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :person/works-for
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :project/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :project/depends-on
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident :person/works-on
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}])

;; File utilities
(defn file-exists? [path]
  (.exists (io/file path)))

(defn read-file-content [path]
  (slurp path))

(defn load-schema-from-file [path]
  "Load schema from file - supports EDN format"
  (let [content (read-file-content path)]
    (read-string content)))

(defn load-data-from-file [path]
  "Load data from file - supports EDN format"
  (let [content (read-file-content path)]
    (read-string content)))

;; Utility functions
(defn capture-output [k]
  (let [out-atom (atom "")
        err-atom (atom "")
        res (atom nil)]
    (binding [*out* (java.io.StringWriter.)
              *err* (java.io.StringWriter.)]
      (try
        (reset! res (k))
        (catch Exception e
          (reset! err-atom (str e))))
      (reset! out-atom (str *out*))
      (reset! err-atom (str @err-atom (str *err*))))
    {:result @res :out @out-atom :err @err-atom}))

(defn text-content [^String s]
  (McpSchema$TextContent. s))

(defn text-result [^String s]
  (McpSchema$CallToolResult. [(text-content s)] false))

(defn format-result [result]
  (if (string? result)
    result
    (pr-str result)))

;; 1. Connect to Database Tool
(def connect-db-schema
  (json/write-str {:type :object
                   :properties {:uri {:type :string
                                     :description "Datomic database URI (e.g., datomic:mem://test, datomic:dev://localhost:4334/mydb)"}
                                :create-if-not-exists {:type :boolean
                                                      :description "Create database if it doesn't exist (default: true)"}}
                   :required [:uri]}))

(defn connect-db-callback [exchange arguments continuation]
  (future
    (let [uri (get arguments "uri")
          create? (get arguments "create-if-not-exists" true)]
      (try
        (when create?
          (d/create-database uri))
        (let [conn (d/connect uri)]
          (reset! conn-atom conn)
          (reset! db-uri-atom uri)
          (continuation (text-result (str "Successfully connected to database: " uri))))
        (catch Exception e
          (continuation (text-result (str "Error connecting to database: " (.getMessage e)))))))))

(def connect-db-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "connect_db" "Connect to a Datomic database" connect-db-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (connect-db-callback exchange arguments #(.success sink %)))))))))

;; 2. Install Schema Tool
(def install-schema-schema
  (json/write-str {:type :object
                   :properties {:schema {:type :string
                                        :description "Schema as EDN string or file path"}
                                :schema-file {:type :string
                                             :description "Path to schema file (EDN format)"}
                                :use-default {:type :boolean
                                             :description "Use default schema (default: false)"}}
                   :required []}))

(defn install-schema-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [schema-str (get arguments "schema")
            schema-file (get arguments "schema-file")
            use-default? (get arguments "use-default" false)
            schema (cond
                     use-default?
                     default-schema

                     ;; Schema file path provided
                     (and schema-file (not (str/blank? schema-file)))
                     (try
                       (if (file-exists? schema-file)
                         (load-schema-from-file schema-file)
                         (throw (Exception. (str "Schema file not found: " schema-file))))
                       (catch Exception e
                         (continuation (text-result (str "Error loading schema file: " (.getMessage e))))
                         nil))

                     ;; Schema string provided
                     (and schema-str (not (str/blank? schema-str)))
                     (try
                       (read-string schema-str)
                       (catch Exception e
                         (continuation (text-result (str "Error parsing schema: " (.getMessage e))))
                         nil))

                     :else
                     (do
                       (continuation (text-result "No schema provided. Use schema, schema-file, or use-default parameters."))
                       nil))]

        (when schema
          (let [{:keys [result err]} (capture-output
                                       #(try
                                          (let [tx-result @(d/transact @conn-atom schema)]
                                            (str "Schema installed successfully. Transaction: " (:tx tx-result)))
                                          (catch Exception e
                                            (str "Error installing schema: " (.getMessage e)))))]
            (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))))

(def install-schema-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "install_schema" "Install schema to the Datomic database" install-schema-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (install-schema-callback exchange arguments #(.success sink %)))))))))

;; 3. Add Data Tool
(def add-data-schema
  (json/write-str {:type :object
                   :properties {:data {:type :string
                                      :description "Data to add as EDN vector of entity maps or file path"}
                                :data-file {:type :string
                                           :description "Path to data file (EDN format)"}}
                   :required []}))

(defn add-data-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [data-str (get arguments "data")
            data-file (get arguments "data-file")
            data (cond
                   ;; Data file path provided
                   (and data-file (not (str/blank? data-file)))
                   (try
                     (if (file-exists? data-file)
                       (load-data-from-file data-file)
                       (throw (Exception. (str "Data file not found: " data-file))))
                     (catch Exception e
                       (continuation (text-result (str "Error loading data file: " (.getMessage e))))
                       nil))

                   ;; Data string provided
                   (and data-str (not (str/blank? data-str)))
                   (try
                     (read-string data-str)
                     (catch Exception e
                       (continuation (text-result (str "Error parsing data: " (.getMessage e))))
                       nil))

                   :else
                   (do
                     (continuation (text-result "No data or data-file provided"))
                     nil))]

        (when data
          (let [{:keys [result err]} (capture-output
                                       #(try
                                          (let [tx-result @(d/transact @conn-atom data)]
                                            (str "Successfully added data. Transaction: " (:tx tx-result)))
                                          (catch Exception e
                                            (str "Error adding data: " (.getMessage e)))))]
            (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))))

(def add-data-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "add_data" "Add data to the Datomic database from string or file" add-data-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (add-data-callback exchange arguments #(.success sink %)))))))))

;; 4. Query Tool
(def query-schema
  (json/write-str {:type :object
                   :properties {:query {:type :string
                                       :description "Datalog query as EDN string"}
                                :args {:type :string
                                      :description "Optional query arguments as EDN vector"}
                                :as-of {:type :string
                                       :description "Optional as-of point (transaction ID or instant)"}}
                   :required [:query]}))

(defn query-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [query-str (get arguments "query")
            args-str (get arguments "args")
            as-of-str (get arguments "as-of")
            {:keys [result err]} (capture-output
                                   #(try
                                      (let [query (read-string query-str)
                                            args (if (and args-str (not (str/blank? args-str)))
                                                   (read-string args-str)
                                                   [])
                                            db (if as-of-str
                                                 (d/as-of (d/db @conn-atom) (read-string as-of-str))
                                                 (d/db @conn-atom))
                                            query-result (apply d/q query db args)]
                                        (format-result query-result))
                                      (catch Exception e
                                        (str "Error executing query: " (.getMessage e)))))]
        (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))

(def query-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "query" "Execute a Datalog query against the database" query-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (query-callback exchange arguments #(.success sink %)))))))))

;; 5. Entity Tool
(def entity-schema
  (json/write-str {:type :object
                   :properties {:id {:type :string
                                    :description "Entity ID or lookup ref as EDN"}
                                :as-of {:type :string
                                       :description "Optional as-of point (transaction ID or instant)"}}
                   :required [:id]}))

(defn entity-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [id-str (get arguments "id")
            as-of-str (get arguments "as-of")
            {:keys [result err]} (capture-output
                                   #(try
                                      (let [entity-id (read-string id-str)
                                            db (if as-of-str
                                                 (d/as-of (d/db @conn-atom) (read-string as-of-str))
                                                 (d/db @conn-atom))
                                            entity (d/entity db entity-id)]
                                        (if entity
                                          (format-result (into {} entity))
                                          "Entity not found"))
                                      (catch Exception e
                                        (str "Error retrieving entity: " (.getMessage e)))))]
        (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))

(def entity-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "entity" "Get entity data by ID or lookup ref" entity-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (entity-callback exchange arguments #(.success sink %)))))))))

;; 6. Find Path Tool
(def find-path-schema
  (json/write-str {:type :object
                   :properties {:from {:type :string
                                      :description "Starting entity ID or lookup ref as EDN"}
                                :to {:type :string
                                    :description "Target entity ID or lookup ref as EDN"}
                                :max-depth {:type :integer
                                           :description "Maximum path depth (default: 5)"}}
                   :required [:from :to]}))

(defn find-paths-between
  "Find all paths between two entities up to max-depth"
  [db from-id to-id max-depth]
  (let [visited (atom #{})
        paths (atom [])]
    (letfn [(dfs [current-id path depth]
              (when (<= depth max-depth)
                (if (= current-id to-id)
                  (swap! paths conj (conj path current-id))
                  (when-not (@visited current-id)
                    (swap! visited conj current-id)
                    (let [entity (d/entity db current-id)]
                      (doseq [[attr value] entity]
                        (when (and (keyword? attr)
                                (not= attr :db/id))
                          (cond
                            ;; Reference attribute - single value
                            (and (number? value) (d/entity db value))
                            (dfs value (conj path current-id attr) (inc depth))

                            ;; Reference attribute - collection
                            (and (coll? value) (every? number? value))
                            (doseq [ref-id value]
                              (when (d/entity db ref-id)
                                (dfs ref-id (conj path current-id attr) (inc depth))))))))
                    (swap! visited disj current-id)))))]
      (dfs from-id [] 0)
      @paths)))

(defn find-path-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [from-str (get arguments "from")
            to-str (get arguments "to")
            max-depth (or (get arguments "max-depth") 5)
            {:keys [result err]} (capture-output
                                   #(try
                                      (let [db (d/db @conn-atom)
                                            from-ref (read-string from-str)
                                            to-ref (read-string to-str)
                                            from-id (if (number? from-ref)
                                                      from-ref
                                                      (:db/id (d/entity db from-ref)))
                                            to-id (if (number? to-ref)
                                                    to-ref
                                                    (:db/id (d/entity db to-ref)))
                                            paths (find-paths-between db from-id to-id max-depth)]
                                        (if (empty? paths)
                                          "No paths found between the entities"
                                          (str "Found " (count paths) " path(s):\n"
                                            (str/join "\n" (map-indexed
                                                             (fn [i path]
                                                               (str (inc i) ". " (pr-str path)))
                                                             paths)))))
                                      (catch Exception e
                                        (str "Error finding path: " (.getMessage e)))))]
        (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))

(def find-path-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "find_path" "Find relationship paths between two entities" find-path-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (find-path-callback exchange arguments #(.success sink %)))))))))

;; 7. History Tool
(def history-schema
  (json/write-str {:type :object
                   :properties {:entity-id {:type :string
                                           :description "Entity ID as EDN"}
                                :attribute {:type :string
                                          :description "Optional attribute to filter history"}
                                :limit {:type :integer
                                       :description "Limit number of history entries (default: 10)"}}
                   :required [:entity-id]}))

(defn history-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [entity-id-str (get arguments "entity-id")
            attribute-str (get arguments "attribute")
            limit (or (get arguments "limit") 10)
            {:keys [result err]} (capture-output
                                   #(try
                                      (let [db (d/db @conn-atom)
                                            entity-id (read-string entity-id-str)
                                            attribute (when attribute-str (read-string attribute-str))
                                            history (d/history db)
                                            query (if attribute
                                                    '[:find ?tx ?added ?v
                                                      :in $ ?e ?a
                                                      :where [?e ?a ?v ?tx ?added]]
                                                    '[:find ?a ?tx ?added ?v
                                                      :in $ ?e
                                                      :where [?e ?a ?v ?tx ?added]])
                                            args (if attribute [history entity-id attribute] [history entity-id])
                                            history-result (take limit (d/q query args))]
                                        (if (empty? history-result)
                                          "No history found for entity"
                                          (str "History entries (limited to " limit "):\n"
                                            (str/join "\n" (map-indexed
                                                             (fn [i entry]
                                                               (str (inc i) ". " (pr-str entry)))
                                                             history-result)))))
                                      (catch Exception e
                                        (str "Error retrieving history: " (.getMessage e)))))]
        (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))

(def history-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "history" "Get transaction history for an entity" history-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (history-callback exchange arguments #(.success sink %)))))))))

;; 8. Load Example Tool
(def load-example-schema
  (json/write-str {:type :object}))

(defn load-example-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [{:keys [result err]} (capture-output
                                   #(do
                                      ;; Install schema
                                      @(d/transact @conn-atom default-schema)
                                      
                                      ;; Add example data
                                      @(d/transact @conn-atom
                                        [{:person/name "Alice"
                                          :person/age 30
                                          :person/email "alice@example.com"}
                                         {:person/name "Bob"
                                          :person/age 25
                                          :person/email "bob@example.com"}
                                         {:person/name "Charlie"
                                          :person/age 35
                                          :person/email "charlie@example.com"}
                                         {:company/name "Tech Corp"}
                                         {:project/name "Project A"}
                                         {:project/name "Project B"}])

                                      ;; Add relationships
                                      @(d/transact @conn-atom
                                        [{:person/name "Alice"
                                          :person/friends [[:person/name "Bob"]]
                                          :person/works-for [:company/name "Tech Corp"]
                                          :person/works-on [[:project/name "Project A"]]}
                                         {:person/name "Bob"
                                          :person/friends [[:person/name "Alice"] [:person/name "Charlie"]]
                                          :person/works-for [:company/name "Tech Corp"]
                                          :person/works-on [[:project/name "Project B"]]}
                                         {:person/name "Charlie"
                                          :person/parent [:person/name "Alice"]
                                          :person/works-for [:company/name "Tech Corp"]}
                                         {:project/name "Project B"
                                          :project/depends-on [[:project/name "Project A"]]}])

                                      "Example database loaded with people, company, and projects"))]
        (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))

(def load-example-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "load_example" "Load example data with people, companies and projects for testing" load-example-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (load-example-callback exchange arguments #(.success sink %)))))))))

;; 9. Database Info Tool
(def db-info-schema
  (json/write-str {:type :object}))

(defn db-info-callback [exchange arguments continuation]
  (future
    (if-not @conn-atom
      (continuation (text-result "Not connected to database. Please run connect_db first."))
      (let [{:keys [result err]} (capture-output
                                   #(try
                                      (let [db (d/db @conn-atom)
                                            basis-t (d/basis-t db)
                                            db-stats (d/db-stats db)]
                                        (str "Database Info:\n"
                                             "URI: " @db-uri-atom "\n"
                                             "Basis T: " basis-t "\n"
                                             "Stats: " (pr-str db-stats)))
                                      (catch Exception e
                                        (str "Error getting database info: " (.getMessage e)))))]
        (continuation (text-result (if (str/blank? err) result (str "Error: " err))))))))

(def db-info-tool
  (McpServerFeatures$AsyncToolSpecification.
    (McpSchema$Tool. "db_info" "Get database information and statistics" db-info-schema)
    (reify java.util.function.BiFunction
      (apply [this exchange arguments]
        (Mono/create
          (reify java.util.function.Consumer
            (accept [this sink]
              (db-info-callback exchange arguments #(.success sink %)))))))))

;; Server setup
(defn mcp-server [& args]
  (let [transport-provider (StdioServerTransportProvider. (ObjectMapper.))
        server (-> (McpServer/async transport-provider)
                 (.serverInfo "datomic-server" "0.1.0")
                 (.capabilities (-> (McpSchema$ServerCapabilities/builder)
                                  (.tools true)
                                  (.build)))
                 (.build))]

    ;; Add all tools
    (doseq [tool [connect-db-tool install-schema-tool add-data-tool query-tool
                  entity-tool find-path-tool history-tool load-example-tool db-info-tool]]
      (-> (.addTool server tool)
        (.subscribe)))

    server))

(defn -main [& args]
  (let [server (mcp-server args)]
    (println "Datomic MCP Server running on STDIO transport.")
    ;; Keep the process alive
    (while true
      (Thread/sleep 1000))))
