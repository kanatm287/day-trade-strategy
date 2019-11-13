(ns financial-data-load.database.pull-from-db
  (:require
    ;; vendor
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; local
    [db.core :as db]))

; ==============================================================================
; ------------------============== Pull tickers ==============------------------

(defn pull-existed-estimates-tickers
  []
  (j/query (db/get-ds)
           (sql/format {:select [:date :ticker :day_trade :short_term]
                        :from [:financial_data]
                        :order-by [:ticker]})))

(defn valid-date-tickers
  []
  (map :ticker
       (filter
         (fn [x]
           (let [date (c/from-sql-time (:date x))
                 interval (t/in-days
                            (t/interval
                              (if (.isAfter (t/now) date) date (t/now))
                              (if (.isAfter (t/now) date) (t/now) date)))]
             (and (not= date nil) (< interval 7))))
         (pull-existed-estimates-tickers))))

(defn pull-valid-tickers
  []
  (remove nil?
          (let [not-valid-tickers (valid-date-tickers)]
            (j/query (db/get-ds)
              (sql/format (merge
                                   {:select [:ticker]
                                    :from [:financial_data]
                                    :order-by [:ticker]}
                                   (when-not (empty? not-valid-tickers)
                                     {:where [:not-in :ticker not-valid-tickers]})))
              {:row-fn :ticker}))))

(defn check-if-trade-params-ticker-exists?
  [ticker]
  (empty?
    (j/query (db/get-ds)
             (sql/format {:select [:ticker]
                          :from [:trade_params]
                          :where [:= :ticker ticker]}))))
