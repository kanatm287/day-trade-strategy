(defproject thin-client "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 ;; voix
                 [ib-client "1.0.0-SNAPSHOT"]
                 ;; vendor
                 [http-kit "2.3.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.slf4j/slf4j-log4j12 "1.7.25"]
                 [overtone/at-at "1.2.0"]
                 [clj-time "0.14.4"]]

  :repl-options {:init-ns thin-client.core}
  :main thin-client.core
  :profiles
  {:dev {:jvm-opts ["-Dib.clientId=2"
                    "-Dib.host=localhost"
                    "-Dib.port=7497"
                    "-Dstrategy.server=http://localhost:7777"]}
   :uberjar {:aot :all}})
