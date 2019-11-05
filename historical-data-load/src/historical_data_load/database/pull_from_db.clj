(ns historical-data-load.database.pull-from-db
  (:require
    ;; vendor
    [clojure.data.json :as json]
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; local
    [db.core :as db]))

; ==============================================================================
; ------------------============== Pull tickers ==============------------------

(defn pull-all-tickers
  []
  (j/query (db/get-ds)
           (sql/format {:select [:ticker]
                        :from [:trade_params]
                        :order-by [:ticker]})
           {:row-fn :ticker}))

(defn pull-tickers
  [column]
  (j/query (db/get-ds)
    (sql/format {:select [:ticker]
                 :from [:trade_params]
                 :where [:and
                         [:not-in :ticker {:select [:ticker]
                                           :from [:illiquid_list]}]
                         [:= column true]]})
    {:row-fn :ticker}))

(defn pull-day-trade-tickers
  []
  (flatten
    (conj (pull-tickers :day_trade_buy)
          (pull-tickers :day_trade_sell))))

(defn pull-trade-params
  [ticker]
  (j/query (db/get-ds)
           (sql/format {:select [:*]
                        :from [:trade_params]
                        :where [:= :ticker ticker]})
           {:result-set-fn first}))

(defn table-contains-ticker?
  [ticker time-frame]
  (empty? (j/query (db/get-ds)
            (sql/format {:select [:ticker]
                         :from [(cond
                                  (= time-frame "minute")
                                  :test_minute_data

                                  (= time-frame "daily")
                                  :historical_daily_data

                                  (= time-frame "test daily")
                                  :test_daily_data

                                  (= time-frame "five minute")
                                  :historical_five_minute_data)]
                         :where [:= :ticker ticker]
                         :limit 1}))))

(defn pull-five-minute-data-tickers
  []
  (j/query (db/get-ds)
           (sql/format {:select    [:ticker]
                        :modifiers [:distinct]
                        :from      [:historical_five_minute_data]
                        :where     [:and
                                    [:not-in :ticker {:select [:benchmark]
                                                      :from   [:benchmark_utils]}]
                                    [:not-in :ticker {:select [:ticker]
                                                      :from   [:illiquid_list]}]]})
           {:row-fn :ticker}))

; ------------------=========== Pull Benchmark Data ===========-----------------

(defn pull-benchmarks
  []
  (j/query (db/get-ds)
           (sql/format {:select [:benchmark :industry :conid]
                        :from [:benchmark_utils]})
           {:result-set-fn vec}))

; ------------------=========== Pull historical data ==========-----------------

(defn pull-n-bars
  [ticker limit time-frame column order]
  (let [select {:select [(keyword column)]
                :from [(cond
                         (= time-frame "minute")
                         :test_minute_data

                         (= time-frame "daily")
                         :historical_daily_data

                         (= time-frame "five minute")
                         :historical_five_minute_data)]
                :where [:= :ticker ticker]
                :order-by [[:date (keyword order)]]}]
    (j/query (db/get-ds)
      (sql/format (if limit
                    (merge select {:limit limit})
                    select)))))

(defn pull-historical-data-by-date
  [ticker date time-frame]
  (j/query (db/get-ds)
           (sql/format {:select [:*]
                        :from [(cond
                                 (= time-frame "minute")
                                 :test_minute_data

                                 (= time-frame "daily")
                                 :historical_daily_data

                                 (= time-frame "five minute")
                                 :historical_five_minute_data)]
                        :where  [:and
                                 [:>= :date (c/to-sql-time (first date))]
                                 [:<= :date (c/to-sql-time (last date))]
                                 [:= :ticker ticker]]
                        :order-by [[:date :asc]]})))

(defn pull-historical-last-data
  [ticker time-frame]
  (j/query (db/get-ds)
           (sql/format {:select [:*]
                        :from [(cond
                                 (= time-frame "minute")
                                 :test_minute_data

                                 (= time-frame "test daily")
                                 :test_daily_data

                                 (= time-frame "daily")
                                 :historical_daily_data

                                 (= time-frame "five minute")
                                 :historical_five_minute_data)]
                        :where [:= :ticker ticker]
                        :order-by [[:date :desc]]
                        :limit 1})
           {:result-set-fn first}))

(defn pull-test-params
  [symbol time-frame strategy action]
  (let [request
        (j/query (db/get-ds)
               (sql/format {:select [:technical_params]
                            :from [:test_params]
                            :where [:and
                                    [:= :symbol symbol]
                                    [:= :time_frame time-frame]
                                    [:= :strategy strategy]
                                    [:= :action action]]})
               {:result-set-fn (comp :technical_params first)})]
    (when request
      (json/read-str request :key-fn keyword))))

; ---------------------========= Absent dates check block =========---------------------

(defn pull-test-dates
  [ticker]
  (j/query (db/get-ds)
           (sql/format {:select [(sql/call :date_trunc
                                           :?period
                                           :date)]
                        :modifiers [:distinct]
                        :from [:test_minute_data]
                        :where [:= :ticker ticker]}
                       :params {:period "day"})
           {:row-fn (fn [{:keys [date_trunc]}]
                      (let [datetime (c/from-sql-time date_trunc)]
                        (t/minus datetime(t/hours (t/hour datetime)))))}))

(defn check-test-minute-dates?
  [ticker date]
  (let [counter
        (j/query (db/get-ds)
                 (sql/format {:select [:%count.date]
                              :from   [:test_minute_data]
                              :where  [:and
                                       [:>= :date (c/to-sql-time (first date))]
                                       [:<= :date (c/to-sql-time (last date))]
                                       [:= :ticker ticker]]})
                 {:result-set-fn (comp :count first)})]
    (and (not= counter 390) (not= counter 0))))

(defn pull-non-full-holidays
  [ticker]
  (j/query (db/get-ds)
           (sql/format {:select [:date]
                        :from [:non_full_holidays]
                        :where [:= :ticker ticker]})
           {:row-fn #(c/from-sql-time (:date %))}))
