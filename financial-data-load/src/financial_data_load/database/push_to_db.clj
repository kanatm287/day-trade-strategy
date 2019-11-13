(ns financial-data-load.database.push-to-db
  (:require
    ;; vendor
    [honeysql.types :as sql-types]
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clj-time.coerce :as c]
    ;; local
    [financial-data-load.database.pull-from-db :as db-pull]
    [db.core :as db]))

(defn update-financial-data
  [ticker {:keys [date estimates future-report-date estimates-params]}]
  (j/execute! (db/get-ds)
              (sql/format {:update :financial_data
                           :set (merge
                                  (when date {:date date})
                                  (when estimates {:estimates estimates})
                                  (when estimates-params
                                    {:estimates_params estimates-params})
                                  (when future-report-date
                                    {:future_report_date
                                     (c/to-sql-time future-report-date)}))
                           :where [:= :ticker ticker]})))

(defn set-column-true
  [tickers column]
  (j/execute! (db/get-ds)
              (sql/format {:update :financial_data
                           :set {column true}
                           :where [:in :ticker tickers]})))

(defn write-ticker-to-db
  [ticker]
  (j/execute! (db/get-ds)
              (sql/format {:insert-into :financial_data
                           :values [{:ticker ticker}]})))

(defn generate-trade-params
  [ticker {:keys [date report-dates last-report-date
                  eps-growth reports day-trade-buy day-trade-sell]}]
  (merge
    {:ticker ticker}
    (when date {:date date})
    (when last-report-date {:last_report_date (c/to-sql-time last-report-date)})
    (when report-dates {:report_dates (sql-types/array report-dates)})
    (when eps-growth {:eps_growth eps-growth})
    (when reports {:reports reports})
    (when day-trade-buy {:day_trade_buy day-trade-buy})
    (when day-trade-sell {:day_trade_sell day-trade-sell})))


(defn write-trade-params
  [ticker trade-params]
  (if (db-pull/check-if-trade-params-ticker-exists? ticker)
    (j/execute! (db/get-ds)
                (sql/format {:insert-into :trade_params
                             :values [(generate-trade-params ticker trade-params)]}))
    (j/execute! (db/get-ds)
                (sql/format {:update :trade_params
                             :set (generate-trade-params ticker trade-params)
                             :where [:= :ticker ticker]}))))
