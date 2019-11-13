(defproject db "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 ;; db
                 [org.postgresql/postgresql "42.2.2"]
                 [hikari-cp "2.4.0"]]

  :repl-options {:init-ns db.core}
  :main db.core
  :profiles
  {:uberjar {:aot :all}})