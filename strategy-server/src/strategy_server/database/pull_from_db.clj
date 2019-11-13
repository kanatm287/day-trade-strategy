(ns strategy-server.database.pull-from-db
  (:require
    ;; vendor
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; local
    [db.core :as db]))

(defn pull-permissions-for-account
  [ib-account-id]
  (first
    (j/query (db/get-ds)
      (sql/format {:select [:bond
                            :day_trade
                            :bond_multiplier
                            :day_trade_multiplier
                            :short_term
                            :short_term_multiplier]
                   :from [:account_permissions]
                   :where [:= :account ib-account-id]}))))

(defn pull-instructions-for-account
  [strategy-id ib-account-id]
  (j/query (db/get-ds)
    (sql/format {:select [:*]
                 :from [:trade_instructions]
                 :where [:and
                         [:> :date (c/to-timestamp (t/today-at-midnight))]
                         [:= :order_status "new"]
                         [:= :account ib-account-id]
                         [:= :strategy_id strategy-id]]})))

(defn pull-filled-market-instructions
  [ib-account-id symbol fill-price]
  (j/query (db/get-ds)
    (sql/format {:select [:*]
                 :from [:trade_instructions]
                 :where [:and
                         [:> :date (c/to-timestamp (t/today-at-midnight))]
                         [:= :order_status "filled"]
                         [:= :account ib-account-id]
                         [:= :symbol symbol]
                         [:= :fill_price fill-price]]
                 :order-by [[:date :desc]]
                 :limit 1})
    {:result-set-fn first}))

(defn pull-instruction-for-order-id
  [ib-account-id order-id]
  (j/query (db/get-ds)
    (sql/format {:select [:instruction_id
                          :signal_id
                          :strategy_id
                          :db_status]
                 :from [:trade_instructions]
                 :where [:and
                         [:= :ib_id order-id]
                         [:> :date (c/to-timestamp (t/today-at-midnight))]
                         [:= :account ib-account-id]]})
    {:result-set-fn first}))

(defn pull-instruction-by-instruction-id
  [instruction-id]
  (first
    (j/query (db/get-ds)
      (sql/format {:select [:*]
                   :from [:trade_instructions]
                   :where [:= :instruction_id instruction-id]}))))

(defn pull-instruction-open-order-quantity
  [oca-group ib-account-id]
  (:quantity
    (first
      (j/query (db/get-ds)
        (sql/format {:select [:*]
                     :from [:trade_instructions]
                     :where [:and
                             [:= :oca_group oca-group]
                             [:= :order_type "MKT"]
                             [:> :date (c/to-timestamp (t/today-at-midnight))]
                             [:= :account ib-account-id]]})))))

; -----------------================= day_trade =================-----------------

(defn pull-tickers-from-day-trade-orders
  []
  (dedupe
    (map :ticker
      (j/query (db/get-ds)
        (sql/format {:select [:ticker]
                     :from [:day_trade_orders]
                     :where [:> :date (c/to-timestamp (t/today-at-midnight))]
                     :order-by [[:ticker :asc]]})))))

(defn pull-orders-from-day-trade-orders
  [ticker]
  (j/query (db/get-ds)
    (sql/format {:select [:*]
                 :from [:day_trade_orders]
                 :where [:and
                         [:= :ticker ticker]
                         [:= :ib_status nil]
                         [:> :date (c/to-timestamp (t/today-at-midnight))]]
                 :order-by [[:date :desc]]
                 :limit 3})))

(defn pull-technical-param
  [ticker strategy column-name action]
  ((keyword column-name)
   (j/query (db/get-ds)
            (sql/format {:select [(keyword column-name)]
                         :from [:test_params]
                         :where [:and
                                 [:= :symbol ticker]
                                 [:= :time_frame "minute"]
                                 [:= :strategy strategy]
                                 [:= :action action]]})
            {:result-set-fn (comp first)})))
