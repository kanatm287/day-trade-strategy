(ns historical-data-load.scripts
  (:require
   ;; Vendor
   [clojure.java.shell :as sh]
   ;; local
   [log.core :as log]
   [db.core :as db]))



(defn db-params
  []
  (let [db-params db/datasource-options
        db-params-string (str (:adapter db-params) "://"
                              (:username db-params) ":"
                              (:password db-params) "@"
                              (:server-name db-params) ":"
                              (:port-number db-params) "/"
                              (:database-name db-params))]
    db-params-string))

(defn prop
  [s]
  (System/getProperty s))

(defn folder-path
  []
  (prop "ib.app-root"))

(defn run-script
  [script-name & args]
  (apply sh/sh (concat
                [(str (folder-path) "/scripts/venv/bin/python")
                 (str (folder-path) "/scripts/" script-name)]
                args)))

(defn prepare-benchmark-data
  [benchmark industry]
  (log/info
    "day-trade"
    "preparing data"
    (str "preparing " benchmark " data industry " industry))
  (run-script
    "historical_prepare_benchmark_data.py"
    (db-params)
    benchmark
    industry))

(defn prepare-counter-data
  []
  (log/info "day-trade" "preparing data" "preparing counter data")
  (run-script "historical_prepare_counter_data.py" (db-params)))

(defn prepare-historical-data
  [ticker]
  (log/info "day-trade"
            "preparing data"
            (str "preparing historical_data_load data for ticker " ticker))
  (run-script "historical_prepare_ticker_data.py" ticker (db-params)))
