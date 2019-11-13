(ns real-time-data-process.database.pull-from-db
  (:require
    ;; vendor
    [clojure.data.json :as json]
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    [clojure.set :as set]
    ;; local
    [db.core :as db]))

; ==============================================================================
; ------------------==== Pull ticker from DB Historicals ====-------------------

(defn pull-existed-trade-tickers
  []
  (j/query (db/get-ds)
           (sql/format {:select [:date :ticker]
                        :from [:trade_params]
                        :order-by [:ticker]})))

(defn not-expired-trade-tickers
  []
  (map :ticker
       (filter
         (fn [x]
           (let [date (c/from-sql-time (:date x))
                 interval (t/in-days
                            (t/interval
                              (if (.isAfter (t/now) date) date (t/now))
                              (if (.isAfter (t/now) date) (t/now) date)))]
             (< interval 7)))
         (pull-existed-trade-tickers))))

(defn pull-valid-tickers
  [column]
  (let [not-expired-trade-tickers (not-expired-trade-tickers)]
    (j/query (db/get-ds)
      (sql/format {:select [:ticker
                            :conid
                            :industry]
                   :from [:trade_params]
                   :where [:and
                           (when-not (empty? not-expired-trade-tickers)
                             [:in :ticker not-expired-trade-tickers])
                           [:= column true]
                           [:in :ticker {:select [:symbol]
                                         :modifiers [:distinct]
                                         :from [:test_params]
                                         :where [:and
                                                 [:= :performance :true]
                                                 [:= :time_frame "minute"]]}]]
                   :order-by [:ticker]}))))

(defn pull-benchmarks
  []
  (j/query (db/get-ds)
           (sql/format {:select [:benchmark
                                 :conid
                                 :industry]
                        :from [:benchmark_utils]})
           {:row-fn #(set/rename-keys % {:benchmark :ticker})}))

; ---------------========== Pull n minute bars from DB ==========---------------

(defn pull-n-minute-bars
  [ticker limit time-frame column order]
  (let [select {:select [(keyword column)]
                :from [(if (= time-frame "5 mins")
                         :historical_five_minute_data
                         :historical_minute_data)]
                :where [:= :ticker ticker]
                :order-by [[:date (keyword order)]]}]
    (j/query (db/get-ds)
      (sql/format (if limit
                    (merge select {:limit limit})
                    select)))))

(defn pull-previous-day-close-bar
  [ticker]
  (j/query (db/get-ds)
           (sql/format {:select [:prev_day_close :date]
                        :from [:historical_minute_data]
                        :where [:= :ticker ticker]
                        :order-by [[:date :desc]]
                        :limit 1})
           {:result-set-fn first}))

(defn get-last-ib-id
  []
  (j/query (db/get-ds)
    (sql/format {:select [:ib_id]
                 :from [:day_trade_orders]
                 :where [:> :date (c/to-timestamp (t/today-at-midnight))]
                 :order-by [[:ib_id :desc]]
                 :limit :1})
    {:result-set-fn (comp first :ib_id)}))

(defn pull-data-from-day-trade-orders
  [ticker order-type signal-id]
  (let [select {:select [:*]
                :from [:day_trade_orders]}
        where (cond
                (and ticker signal-id)
                {:where [:and
                         [:= :ticker ticker]
                         [:> :date (c/to-timestamp (t/today-at-midnight))]]
                 :order-by [[:ib_id :desc]]
                 :limit 3}
                (and ticker order-type)
                {:where [:and
                         [:= :ticker ticker]
                         [:> :date (c/to-timestamp (t/today-at-midnight))]]
                 :order-by [[:ib_id :desc]]}
                order-type
                {:where [:and
                         [:= :order_type order-type]
                         [:> :date (c/to-timestamp (t/today-at-midnight))]]}
                ticker
                {:where [:and
                         [:= :ticker ticker]
                         [:> :date (c/to-timestamp (t/today-at-midnight))]
                         [:= :signal_id signal-id]]})]
    (j/query (db/get-ds)
      (sql/format (merge select where)))))

; -------------========== Pull technical params from DB ==========--------------

(defn pull-test-params
  [ticker strategy column action]
  (json/read-str
    (j/query (db/get-ds)
      (sql/format {:select [(keyword column)]
                   :from [:test_params]
                   :where [:and
                           [:= :symbol ticker]
                           [:= :time_frame "minute"]
                           [:= :strategy strategy]
                           [:= :action action]]})
      {:result-set-fn (comp :technical_params first)})
    :key-fn keyword))

(defn pull-fundamental-params
  [ticker]
  (j/query (db/get-ds)
           (sql/format {:select [:*]
                        :from [:trade_params]
                        :where [:= :ticker ticker]})
           {:result-set-fn first}))
