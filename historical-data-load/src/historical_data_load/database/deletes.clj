(ns historical-data-load.database.deletes
  (:require
    ;; vendor
    [clojure.java.jdbc :as j]
    [clj-time.coerce :as c]
    [clj-time.format :as f]
    [honeysql.core :as sql]
    ;; local
    [db.core :as db]
    [clj-time.core :as t]))

(defn delete-all-hist-data
  []
  (j/execute! (db/get-ds) ["TRUNCATE TABLE logs"])
  (j/execute! (db/get-ds) ["TRUNCATE TABLE logs_execution"])
  (j/execute! (db/get-ds) ["TRUNCATE TABLE logs_orders"])
  (j/execute! (db/get-ds) ["TRUNCATE TABLE logs_server"])
  (j/execute! (db/get-ds) ["TRUNCATE TABLE day_trade_orders"])
  (j/execute! (db/get-ds) ["TRUNCATE TABLE historical_daily_temporary_data"])
  (j/execute! (db/get-ds) ["TRUNCATE TABLE historical_minute_data"]))

(defn remove-from-trade-params
  [ticker]
  (j/execute! (db/get-ds)
    (sql/format {:delete-from :trade_params
                 :where [:= :ticker ticker]})))

(defn synchronize-historical-and-test-data
  [ticker start-date last-date time-frame]
  (let [table (if (= time-frame "daily")
                :historical_daily_temporary_data
                :test_minute_data)]
    (j/execute! (db/get-ds)
      (sql/format {:delete-from table
                   :where [:and
                           [:> :date last-date]
                           [:= :ticker ticker]]}))
    (j/execute! (db/get-ds)
      (sql/format {:delete-from table
                   :where [:and
                           [:<= :date start-date]
                           [:= :ticker ticker]]}))))

(defn delete-non-full-day
  [date ticker]
  (j/execute! (db/get-ds)
    (sql/format {:delete-from :historical_daily_temporary_data
                 :where [:and
                         [:= :date (c/to-timestamp
                                     (f/parse
                                       (f/formatter "yyyyMMdd")
                                       date))]
                         [:= :ticker ticker]]})))

(defn clean-hist-temp-data
  [ticker]
  (j/execute! (db/get-ds)
    (sql/format {:delete-from :historical_daily_temporary_data
                 :where [:= :ticker ticker]})))

(defn delete-two-year-excess-historical-daily-data
  [ticker]
  (j/execute! (db/get-ds)
    (sql/format {:delete-from :historical_daily_data
                 :where [:and
                         [:<= :date (c/to-sql-time
                                      (t/minus
                                        (t/now)
                                        (t/years 2)))]
                         [:= :ticker ticker]]})))

(defn delete-two-year-excess-rows
  [ticker]
  (j/execute! (db/get-ds)
              (sql/format {:delete-from :historical_five_minute_data
                           :where [:<= :date {:select [:date]
                                              :from [:historical_five_minute_data]
                                              :where [:= :ticker ticker]
                                              :order-by [[:date :desc]]
                                              :limit 1
                                              :offset (* 504 78)}]})))
