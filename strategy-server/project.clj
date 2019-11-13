(defproject strategy-server "0.1.0-SNAPSHOT"
  :dependencies [;; main
                 [org.clojure/clojure "1.9.0"]
                 ;; http server
                 [http-kit "2.3.0"]
                 [compojure "1.6.1"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-json "0.4.0"]
                 [org.clojure/data.json "0.2.6"]
                 [metosin/ring-http-response "0.9.0"]
                 ;; db
                 [honeysql "0.9.2"]
                 ;; misc
                 [clj-time "0.14.4"]
                 [overtone/at-at "1.2.0"]
                 ;; local
                 ;; log
                 [log "0.1.0-SNAPSHOT"]
                 ;; db
                 [db "0.1.0-SNAPSHOT"]
                 ;;
                 [bond "0.1.0-SNAPSHOT"]]

  :exclusions [org.clojure/clojurescript]

  :repl-options {:init-ns strategy-server.core}
  :main strategy-server.core
  :profiles
  {:dev {:jvm-opts ["-Dibdb.host=localhost"
                    "-Dibdb.name=my_database"
                    "-Dibdb.user=postgres"
                    "-Dibdb.password=postgres"
                    "-Dib.port=4001"]}
   :uberjar {:aot :all}})
