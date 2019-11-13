(defproject real-time-data-process "0.1.0-SNAPSHOT"
  :dependencies [;; main
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "0.3.443"]
                 [org.clojure/data.json "0.2.6"]
                 ;; ib
                 [ib-client "1.0.0-SNAPSHOT"]
                 [log "0.1.0-SNAPSHOT"]
                 ;; db
                 [db "0.1.0-SNAPSHOT"]
                 [honeysql "0.9.1"]
                 ;; misc
                 [clj-time "0.14.0"]
                 [overtone/at-at "1.2.0"]]

  :repl-options {:init-ns real-time-data-process.core}
  :main real-time-data-process.core
  :profiles
  {:dev {:jvm-opts ["-Dibdb.host=localhost"
                    "-Dibdb.port=5432"
                    "-Dibdb.name=my-database"
                    "-Dibdb.user=postgres"
                    "-Dibdb.password=postgres"
                    "-Dib.clientId=1"
                    "-Dib.host=localhost"
                    "-Dib.port=4001"
                    "-Dib.app-root=/app-root"]}
   :uberjar {:aot :all}})
