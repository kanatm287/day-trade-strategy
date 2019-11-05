(defproject historical-data-load "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/data.json "0.2.6"]
                 [ib-client "1.0.0-SNAPSHOT"]
                 [org.clojure/java.jdbc "0.7.1"]
                 [org.postgresql/postgresql "42.1.4"]
                 [db "0.1.0-SNAPSHOT"]
                 [honeysql "0.9.1"]
                 [clj-time "0.14.0"]
                 [overtone/at-at "1.2.0"]
                 [log "0.1.0-SNAPSHOT"]
                 [org.clojure/data.xml "0.0.8"]
                 [com.rpl/specter "1.1.2"]]

  :repl-options {:init-ns historical-data-load.core}
  :main historical-data-load.core
  :profiles
  {:dev {:jvm-opts ["-Dibdb.host=localhost"
                    "-Dibdb.name=my_database"
                    "-Dibdb.user=postgres"
                    "-Dibdb.password=postgres"
                    "-Dibdb.port=5432"
                    "-Dib.clientId=1"
                    "-Dib.host=localhost"
                    "-Dib.port=4001"
                    "-Dib.app-root=/app-root"]}
   :uberjar {:aot :all}})
