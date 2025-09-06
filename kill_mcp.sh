lsof -i :9889 | grep LISTEN | awk '{print $2}' | xargs kill -9
kill -9 `ps aux | grep java | grep datomic-mcp.core | awk '{print $2}'`

ps aux | grep java
