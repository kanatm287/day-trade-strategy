(ns strategy-server.database.db-querys
  (:require
    ;; vendor
    [clj-time.coerce :as c]
    [clj-time.core :as t]))

(defn create-write-query
  [{:keys [order-status
           signal-id
           strategy-id
           symbol
           sec-type
           expire
           strike
           multiplier
           currency
           order-type
           quantity
           tif
           transmit
           account
           put-or-call
           buy-or-sell
           limit-price
           stop-price
           oca-group
           date
           cll-int-id
           conids]}]
  {:insert-into :trade_instructions
   :values [{:order_status order-status
             :signal_id signal-id
             :strategy_id strategy-id
             :symbol symbol
             :security_type sec-type
             :expire (when expire expire)
             :strike (when strike strike)
             :multiplier (when multiplier multiplier)
             :currency currency
             :order_type order-type
             :quantity quantity
             :time_in_force tif
             :transmit transmit
             :account account
             :db_status "new"
             :put_or_call (when put-or-call put-or-call)
             :buy_or_sell (when buy-or-sell buy-or-sell)
             :limit_price (when limit-price limit-price)
             :stop_price (when stop-price stop-price)
             :oca_group (when oca-group oca-group)
             :date date
             :cll_int_id (when cll-int-id (if (string? cll-int-id)
                                            (Integer/parseInt cll-int-id)
                                            cll-int-id))
             :conids (when conids conids)}]})

(defn create-execution-query
  [id
   {:keys [symbol
           sec-type
           expire
           strike
           right
           exchange
           currency
           combo-legs]}
   {:keys [exec-id
           account
           side
           quantity
           price
           cum-quant
           avg-price
           order-reference]}
   {:keys [instruction_id
           signal_id]}]
  {:insert-into :trade_executions
   :values [{:ib_id id
             :symbol symbol
             :sec_type sec-type
             :expire (when expire expire)
             :strike (when strike strike)
             :put_or_call (when right right)
             :exchange exchange
             :currency currency
             :combo_legs combo-legs
             :execution_id exec-id
             :account account
             :buy_or_sell (when side side)
             :quantity quantity
             :price price
             :cum_quantity cum-quant
             :average_price avg-price
             :order_reference order-reference
             :instruction_id instruction_id
             :signal_id signal_id
             :date (c/to-timestamp (t/now))}]})

(def create-pull-query
  {:select [:*]
   :from [:trade_instructions]
   :where [:and]})