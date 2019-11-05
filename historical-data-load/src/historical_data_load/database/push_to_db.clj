(ns historical-data-load.database.push-to-db
  (:require
    ;; vendor
    [honeysql.types :as sql-types]
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clj-time.coerce :as c]
    [clj-time.format :as f]
    [clj-time.core :as t]
    ;; local
    [db.core :as db]))

; ==============================================================================
; ----------------============== Day Trading Data ==============----------------

(defn update-day-trade-buy
  []
  (j/execute! (db/get-ds)
    (sql/format {:update :trade_params
                 :set {:day_trade_buy false}
                 :where [:not-in
                         :ticker
                         {:select [:ticker]
                          :from [:trade_params]
                          :where [:and
                                  [:= :day_trade_buy :true]
                                  [:> :last_price :three_quarter_average_price]]}]})))

(defn update-day-trade-sell
  []
  (j/execute! (db/get-ds)
    (sql/format {:update :trade_params
                 :set {:day_trade_sell false}
                 :where [:not-in
                         :ticker
                         {:select [:ticker]
                          :from [:trade_params]
                          :where [:and
                                  [:= :day_trade_sell :true]
                                  [:< :last_price :three_quarter_average_price]]}]})))

(defn update-trade-params
  [ticker {:keys [date report-dates estimates
                  three-quarter-average_price last_price
                  last-report-date
                  target-price last-report-price
                  eps-growth reports day-trade-buy day-trade-sell
                  vol-max vol-min
                  min-tick conid industry]}]
  (j/execute! (db/get-ds)
   (sql/format {:update :trade_params
                :set (merge
                       (when date {:date date})
                       (when estimates {:estimates estimates})
                       (when three-quarter-average_price
                         {:three_quarter_average_price
                          three-quarter-average_price})
                       (when last_price {:last_price last_price})
                       (when last-report-date
                         {:last_report_date (c/to-sql-time last-report-date)})
                       (when target-price {:target_price target-price})
                       (when last-report-price
                         {:last_report_price last-report-price})
                       (when eps-growth {:eps_growth eps-growth})
                       (when reports {:reports reports})
                       (when day-trade-buy {:day_trade_buy day-trade-buy})
                       (when day-trade-sell {:day_trade_sell day-trade-sell})
                       (when vol-max {:vol_max vol-max})
                       (when vol-min {:vol_min vol-min})
                       (when min-tick {:min_tick min-tick})
                       (when conid {:conid conid})
                       (when industry {:industry industry})
                       (when report-dates
                         {:report_dates (sql-types/array report-dates)}))
                :where [:= :ticker ticker]})))

(defn set-benchmark-params
  [benchmark conid]
  (j/execute! (db/get-ds)
              (sql/format {:update :benchmark_utils
                           :set {:conid conid}
                           :where [:= :benchmark benchmark]})))

(defn set-volume-max-min
  [ticker]
  (j/execute! (db/get-ds)
    (sql/format {:update :trade_params
                 :set {:vol_max {:select [:%max.volume]
                                 :from [:historical_five_minute_data]
                                 :where [:= :ticker ticker]}
                       :vol_min {:select [:%min.volume]
                                 :from [:historical_five_minute_data]
                                 :where [:= :ticker ticker]}}
                 :where [:= :ticker ticker]})))

;;-------------------================= Test =================-------------------

(defn add-ticker-to-test-data
  [ticker time-frame strategy action]
  (j/execute! (db/get-ds)
    (sql/format {:insert-into :test_params
                 :values [{:symbol ticker
                           :time_frame time-frame
                           :strategy strategy
                           :action action}]})))

(defn write-ticker-to-illiquid-list
  [ticker]
  (let [exists (j/query (db/get-ds)
                 (sql/format {:select [:date]
                              :from [:illiquid_list]
                              :where [:= :ticker ticker]})
                 {:result-set-fn (comp :date first)})]
    (if exists
      (j/execute! (db/get-ds)
        (sql/format {:update :illiquid_list
                     :set {:date (c/to-sql-time (t/now))}
                     :where [:= :ticker ticker]}))
      (j/execute! (db/get-ds)
       (sql/format {:insert-into :illiquid_list
                    :values [{:date (c/to-sql-time (t/now))
                              :ticker ticker}]})))))

(defn add-non-full-day
  [ticker date]
  (j/execute! (db/get-ds)
    (sql/format {:insert-into :non_full_holidays
                 :values [{:date (c/to-timestamp
                                   (f/parse
                                     (f/formatter "yyyyMMdd")
                                     date))
                           :ticker ticker}]})))

(defn copy-hist-to-temp
  [ticker]
  (j/execute! (db/get-ds)
   (conj [(str "INSERT INTO historical_data_temp "
               "SELECT * FROM historical_daily_data "
               "WHERE ticker = ?")]
         ticker)))
