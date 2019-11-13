(ns strategy-server.database.push-to-db
  (:require
    ;; vendor
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clojure.string :as string]
    ;; local
    [db.core :as db]
    [strategy-server.database.db-querys :as exec]
    [clj-time.coerce :as c]
    [clj-time.core :as t]))

(defn pull-instruction-by-cll-int-id
  [cll-id]
  (first
    (j/query (db/get-ds)
      (sql/format
        (update exec/create-pull-query :where #(conj % [:= :cll_int_id cll-id]))))))

(defn signal-written?
  [{:keys [cll-int-id strategy-id account symbol]}]
  (let [count (j/query (db/get-ds)
                (sql/format
                  {:select [[:%count.* "count"]]
                   :from [:trade_instructions]
                   :where [:and
                           [:= :symbol symbol]
                           [:= :strategy_id strategy-id]
                           [:= :cll_int_id (if (string? cll-int-id)
                                             (db/as-int cll-int-id)
                                             cll-int-id)]
                           [:= :account account]
                           [:> :date (c/to-timestamp
                                       (t/today-at-midnight))]]})
                {:result-set-fn first
                 :row-fn :count})]
    (> count 0)))

(defn signal-executed?
  [{:keys [cll-int-id strategy-id account symbol]}]
  (let [status
        (:order_status (first
                         (j/query (db/get-ds)
                           (sql/format
                             {:select [:order_status]
                              :from [:trade_instructions]
                              :where [:and
                                      [:= :symbol symbol]
                                      [:= :strategy_id strategy-id]
                                      [:= :cll_int_id (if (string? cll-int-id)
                                                        (db/as-int cll-int-id)
                                                        cll-int-id)]
                                      [:= :account account]
                                      [:> :date (c/to-timestamp
                                                  (t/today-at-midnight))]]}))))]
    (or (= status "filled") (= status "cancelled") (= status "inactive"))))

(defn write-instruction-to-db
  [instruction]
  (if (signal-written? instruction)
    (when-not (signal-executed? instruction)
      (cond
        (= (string/lower-case (:order-status instruction)) "canceled")
        (j/execute! (db/get-ds)
          (sql/format
            {:update :trade_instructions
             :set {:order_status "canceled"}
             :where [:= :cll_int_id (:cll-int-id instruction)]}))

        (and (= (:strategy-id instruction) "day-trade")
             (= (:order-status instruction) "new")
             (= (:order-type instruction) "LMT")
             (not= (:limit_price (pull-instruction-by-cll-int-id
                                   (:cll-int-id instruction)))
                   (:limit-price instruction)))
        (j/execute! (db/get-ds)
                    (sql/format
                      {:update :trade_instructions
                       :set {:limit_price (:limit-price instruction)
                             :order-status "new"}
                       :where [:= :cll_int_id (:cll-int-id instruction)]}))

        (and (= (:strategy-id instruction) "day-trade")
             (= (:order-status instruction) "new")
             (= (:order-type instruction) "STP")
             (not= (:stop_price (pull-instruction-by-cll-int-id
                                  (:cll-int-id instruction)))
                   (:stop-price instruction)))
        (j/execute! (db/get-ds)
                    (sql/format
                      {:update :trade_instructions
                       :set {:stop_price (:stop-price instruction)
                             :order-status "new"}
                       :where [:= :cll_int_id (:cll-int-id instruction)]}))))
    (j/execute! (db/get-ds) (sql/format (exec/create-write-query instruction)))))

(defn execution-written?
  [{:keys [exec-id]}]
  (let [count (j/query (db/get-ds)
                (sql/format
                  {:select [[:%count.* "count"]]
                   :from [:trade_executions]
                   :where [:and
                           [:= :execution_id exec-id]
                           [:> :date (c/to-timestamp
                                       (t/today-at-midnight))]]})
                {:result-set-fn first
                 :row-fn :count})]
    (> count 0)))

(defn write-commission-to-db
  [{:keys [exec-id commission realized-profit-loss yield yield-date]}]
  (loop []
    (if (execution-written? {:exec-id exec-id})
      (j/execute! (db/get-ds)
        (sql/format
          {:update :trade_executions
           :set {:commission commission
                 :realized_profit_loss realized-profit-loss
                 :yield yield
                 :yield-date yield-date}
           :where [:= :execution_id exec-id]}))
      (do
        (Thread/sleep 500)
        (recur)))))

(defn write-execution-to-db
  [id contract execution ins]
  (if (execution-written? execution)
    (j/execute! (db/get-ds)
      (sql/format
        {:update :trade_executions
         :set {:quantity (:quantity execution)
               :price (:price execution)
               :cum_quantity (:cum-quant execution)
               :average_price (:avg-price execution)}
         :where [:and
                 [:= :execution_id (:exec-id execution)]
                 [:= :ib_id id]
                 [:= :instruction_id (:instruction_id ins)]]}))
    (j/execute! (db/get-ds)
      (sql/format
        (exec/create-execution-query
          id contract execution ins)))))

(defn update-instruction-status
  [instruction-id fill-price status]
  (j/execute! (db/get-ds)
    (sql/format {:update :trade_instructions
                 :set {:order_status status
                       :db_status status
                       :fill_price fill-price}
                 :where [:= :instruction_id instruction-id]})))

(defn update-instruction-ib-id
  [instruction-id ib-id]
  (j/execute! (db/get-ds)
    (sql/format {:update :trade_instructions
                 :set {:ib_id ib-id}
                 :where [:= :instruction_id instruction-id]})))

; -------------------=============== Day trade ===============-------------------

(defn update-day-trade-orders
  [id oca-group fill-price]
  (j/execute! (db/get-ds)
    (sql/format {:update :day_trade_orders
                 :set {:fill_price fill-price}
                 :where [:and
                         [:= :oca_group oca-group]
                         [:= :ib_id id]]})))
