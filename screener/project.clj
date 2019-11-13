(defproject screener "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.jsoup/jsoup "1.11.3"]
                 [dk.ative/docjure "1.12.0"]]

  :repl-options {:init-ns screener.core}
  :main screener.core
  :profiles
  {:dev {:jvm-opts ["-Dibdb.host=localhost"
                    "-Dibdb.name=my_database"
                    "-Dibdb.port=5432"
                    "-Dibdb.user=postgres"
                    "-Dibdb.password=postgres"
                    "-Dib.app-root=/app-root"]}
   :uberjar {:aot :all}})
