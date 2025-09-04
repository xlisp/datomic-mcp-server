
# Datomic MCP Server


- [x] Datomic in-memory database already supports
- [ ] Connect any Datomic database url

## Run in JDK 17

```
export JAVA_HOME="/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home" #'/Library/Java/JavaVirtualMachines/microsoft-11.jdk/Contents/Home'
export PATH=$JAVA_HOME/bin:$PATH
export CPPFLAGS="-I/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/include"

```

## Run datomic transactor

* datomic-pro-1.0.6165 transactor must run on java 1.8

```

➜  datomic-pro-1.0.6165 java -version
openjdk version "17.0.16" 2025-07-15 LTS
OpenJDK Runtime Environment Microsoft-11926165 (build 17.0.16+8-LTS)
OpenJDK 64-Bit Server VM Microsoft-11926165 (build 17.0.16+8-LTS, mixed mode, sharing)
➜  datomic-pro-1.0.6165

➜  datomic-pro-1.0.6165 ./rundatomic.sh
Launching with Java options -server -Xms4g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=50
Starting datomic:sql://<DB-NAME>?jdbc:postgresql://localhost:5432/db123123?user=postgres&password=123456, you may need to change the user and password parameters to work with your jdbc driver ...

```
