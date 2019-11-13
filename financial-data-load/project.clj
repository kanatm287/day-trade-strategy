(defproject financial-data-load "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/data.json "0.2.6"]

                 [org.postgresql/postgresql "42.1.4"]
                 [org.clojure/java.jdbc "0.7.1"]
                 [honeysql "0.9.1"]

                 [org.clojure/data.xml "0.0.8"]

                 [overtone/at-at "1.2.0"]
                 [clj-time "0.14.0"]

                 [ib-client "1.0.0-SNAPSHOT"]
                 [screener "0.1.0-SNAPSHOT"]
                 [log "0.1.0-SNAPSHOT"]
                 [db "0.1.0-SNAPSHOT"]]

  :repl-options {:init-ns financial-data-load.core}
  :main financial-data-load.core
  :profiles
  {:dev {:jvm-opts ["-Dibdb.host=localhost"
                    "-Dibdb.name=my_database"
                    "-Dibdb.user=postgres"
                    "-Dibdb.password=postgres"
                    "-Dibdb.port=5432"
                    "-Dib.clientId=1"
                    "-Dib.host=localhost"
                    "-Dib.port=4002"]}
   :uberjar {:aot :all}})