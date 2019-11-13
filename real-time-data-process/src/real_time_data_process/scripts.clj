(ns real-time-data-process.scripts
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

(defn scripts-path
  []
  (let [path (prop "ib.app-root")]
    (str path "/scripts")))

(defn run-script
  [script-name & args]
  (apply sh/sh (concat
                [(str (scripts-path) "/venv/bin/python")
                 (str (scripts-path) "/" script-name)]
                args)))

(defn machine-learning-functions
  [ticker]
  (log/info
    "day trade"
    "predicting day trade extremums for ticker "
    ticker)
  (run-script "ml_classify_predict.py" ticker (db-params)))

(defn prepare-real-time-counter-data
  []
  (log/info
    "day trade"
    "preparing real time counter data "
    nil)
  (run-script "realtime_prepare_counter_data.py" (db-params)))

(defn prepare-real-time-benchmark-data
  []
  (log/info
    "day trade"
    "preparing real time benchmark data "
    nil)
  (run-script "realtime_prepare_benchmark_data.py" (db-params)))

(defn prepare-real-time-data
  [ticker time]
  (log/info "day trade" (str "preparing real time " time " data for ")
            (str ticker " time frame " time))
  (run-script "realtime_prepare_ticker_data.py" ticker time (db-params)))
