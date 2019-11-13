(ns thin-client.execution
  (:require
    ;;local
    [ib-client.state :as client-state]
    [ib-client.client :as ib-client]
    [thin-client.exec-utils :as exec]
    [thin-client.http-request :as reqs]))

(defn execute
  [req-id contract instruction order-params]
  (exec/add
   {:id req-id
    :instruction-id (:instruction_id instruction)
    :status :new
    :signal-id (:signal_id instruction)
    :strategy-id (:strategy instruction)
    :contract contract
    :order order-params
    :time-in-force (:time_in_force instruction)
    :db-status :new}))

(defn check-modify-or-execute
  [req-id contract instruction order-params modify-order]
  (if modify-order
    (execute req-id contract instruction order-params)
    (when
      (reqs/update-ib-id
        {:instruction-id (:instruction_id instruction)
         :ib-id req-id})
      (execute req-id contract instruction order-params))))

(defn execute-market-spot-instruction
  [instruction]
  (let [contract (ib-client/create-contract
                  {:symbol (:symbol instruction)
                   :sec-type (:security_type instruction)
                   :conid (when (:conids instruction)
                            (Integer/parseInt (:conids instruction)))
                   :exchange "SMART"})
        req-id (client-state/new-req-id @exec/api-atom)
        order-params {:order-type (:order_type instruction)
                      :transmit (:transmit instruction)
                      :action (:buy_or_sell instruction)
                      :total-quantity (:quantity instruction)
                      :account (:account instruction)}
        modify-order false]
    (check-modify-or-execute
      req-id contract instruction order-params modify-order)))

(defn execute-limit-spot-instruction
  [instruction]
  (let [contract (ib-client/create-contract
                   {:symbol (:symbol instruction)
                    :sec-type (:security_type instruction)
                    :conid (Integer/parseInt (:conids instruction))
                    :exchange "SMART"})
        req-id (if (:ib_id instruction)
                 (:ib_id instruction)
                 (client-state/new-req-id @exec/api-atom))
        modify-order (if (:ib_id instruction) true false)
        order-params {:order-type (:order_type instruction)
                      :limit-price (:limit_price instruction)
                      :transmit (:transmit instruction)
                      :action (:buy_or_sell instruction)
                      :total-quantity (:quantity instruction)
                      :oca-group (:oca_group instruction)
                      :account (:account instruction)}]
    (check-modify-or-execute
      req-id contract instruction order-params modify-order)))

(defn execute-stop-spot-instruction
  [instruction]
  (let [contract (ib-client/create-contract
                   {:symbol (:symbol instruction)
                    :sec-type (:security_type instruction)
                    :conid (Integer/parseInt (:conids instruction))
                    :exchange "SMART"})
        req-id (if (:ib_id instruction)
                 (:ib_id instruction)
                 (client-state/new-req-id @exec/api-atom))
        modify-order (if (:ib_id instruction) true false)
        stop-price (if (= (:strategy_id instruction) "day_trade")
                     {:order-type "TRAIL"
                      :trailing-percent (:stop_price instruction)}
                     {:stop-price (:stop_price instruction)
                      :order-type (:order_type instruction)})
        order-params {:transmit (:transmit instruction)
                      :action (:buy_or_sell instruction)
                      :total-quantity (:quantity instruction)
                      :oca-group (:oca_group instruction)
                      :account (:account instruction)}
        order (merge stop-price order-params)]
    (check-modify-or-execute
      req-id contract instruction order modify-order)))

(defn execute-limit-option-instruction
  [contract instruction]
  (let [order-params {:order-type "LMT"
                      :transmit (:transmit instruction)
                      :action (:buy_or_sell instruction)
                      :limit-price (Double/parseDouble
                                     (format "%.2f"
                                             (:limit_price instruction)))
                      :total-quantity (:quantity instruction)
                      :account (:account instruction)}]
    (check-modify-or-execute
      (:ib_id instruction) contract instruction order-params true)))
