(ns back-test.scripts
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

(defn prepare-historical-data
  [ticker]
  (log/info "day-trade"
            "preparing data"
            (str "preparing machine learning historical data for ticker "
                 ticker))
  (run-script "ml_prepare_historical_data.py"
              ticker (db-params)))

(defn classify-data
  [ticker]
  (log/info "day trade" "clussifying data" ticker)
  (run-script "ml_classify_fit.py" ticker (db-params)))

(defn predict-five-minute-back-test-data
  [ticker]
  (log/info "zipline" "back test"
            (str "predict machine learning back test data for "
                 ticker))
  (run-script
    "ml_predict_back_test_data.py" ticker (db-params)))

(defn zipline-back-test-minute
  [ticker action strategy option]
  (log/info "zipline" "back test"
            (str "back test day trade for ticker " ticker " " strategy " " option))
  (run-script (str "back_test_day_trade_" action ".py")
              ticker
              (db-params)
              strategy
              option))
