(defproject log "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.1"]
                 [honeysql "0.9.1"]
                 [clj-time "0.14.0"]]

  :repl-options {:init-ns log.core}
  :main log.core
  :profiles
  {:uberjar {:aot :all}})