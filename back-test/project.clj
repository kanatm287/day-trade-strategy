(defproject back-test "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.1"]
                 [org.postgresql/postgresql "42.1.4"]
                 [default/historical-data "0.1.0-SNAPSHOT"]
                 [honeysql "0.9.1"]
                 [clj-time "0.14.0"]
                 [db "0.1.0-SNAPSHOT"]
                 [log "0.1.0-SNAPSHOT"]
                 [ib-client "1.0.0-SNAPSHOT"]]

  :repl-options {:init-ns back-test.core}
  :main back-test.core
  :profiles
  {:dev {:jvm-opts ["-Dibdb.host=localhost"
                    "-Dibdb.name=my_database"
                    "-Dibdb.port=5432"
                    "-Dibdb.user=postgres"
                    "-Dibdb.password=postgres"
                    "-Dib.app-root=/app-root"
                    "-Xms512m"
                    "-Xmx4G"]}
   :uberjar {:aot :all}})