(ns thin-client.strat.core
  (:require [thin-client.utils :as u]
            [ib-client.client :as ib-client]
            [thin-client.execution :as execution]
            [thin-client.http-request :as reqs]
            [thin-client.strat.auction :as auction]
            [thin-client.exec-utils :refer [api-atom]]))

(def mkt-ins (atom []))
(def stp-ins (atom []))
(def lmt-ins (atom []))

(defn get-positions
  [ib-account-id]
  (let [all-accounts-pos (map :data (ib-client/request-positions @api-atom))]
    (when-not (empty? all-accounts-pos)
      (filter (fn [x] (= (:account x) ib-account-id))
              all-accounts-pos))))

(defn process-market-instruction
  [ins]
  (let [positions (get-positions (:account ins))]
    (cond
      (and (= (:security_type ins) "STK")
           (boolean (if (= (:symbol ins) "HYG")
                      (when positions (u/position-exists? ins positions))
                      (not (when positions (u/position-exists? ins positions)))))
           (boolean (not (u/open-order-exists? ins))))
      (execution/execute-market-spot-instruction ins)

      (and (or (= (:security_type ins) "OPT")
               (= (:security_type ins) "BAG"))
           (boolean (not (when positions (u/position-exists? ins positions))))
           (boolean (not (u/open-order-exists? ins))))
      (auction/market-instruction ins)

      :else  nil)))

(defn process-stop-instruction
  [ins]
  (let [positions (get-positions (:account ins))]
    (when (and (= (:security_type ins) "STK")
               (boolean (when positions (u/position-exists? ins positions))))
      (if (:ib_id ins)
        (if (boolean (u/close-order-id-exists? ins))
          (execution/execute-stop-spot-instruction ins)
          (reqs/update-ib-id
            {:instruction-id (:instruction_id ins)
             :ib-id nil}))
        (execution/execute-stop-spot-instruction ins)))))

(defn process-limit-instruction
  [ins]
  (let [positions (get-positions (:account ins))]
    (cond
      (and (= (:security_type ins) "STK")
           (boolean (when positions (u/position-exists? ins positions))))
      (if (:ib_id ins)
        (if (u/close-order-id-exists? ins)
          (execution/execute-limit-spot-instruction ins)
          (reqs/update-ib-id
            {:instruction-id (:instruction_id ins)
             :ib-id nil}))
        (execution/execute-limit-spot-instruction ins))

      (and (or (= (:security_type ins) "OPT")
               (= (:security_type ins) "BAG"))
           (boolean (not (u/open-order-exists? ins))))
      (auction/limit-instruction ins)

      :else nil)))

(defn execute-mkt-orders
  [_ a _ new-state]
  (when-not (empty? new-state)
    (let [ins (first new-state)]
      (process-market-instruction ins)
      (reset! mkt-ins
              (filter
                #(not= ins %)
                (distinct (flatten (conj new-state mkt-ins))))))))

(add-watch mkt-ins :mkt-orders-watch execute-mkt-orders)

(defn execute-stp-orders
  [_ a _ new-state]
  (when-not (empty? new-state)
    (let [ins (first new-state)]
      (process-stop-instruction ins)
      (reset! stp-ins
              (filter
                #(not= ins %)
                (distinct (flatten (conj new-state @stp-ins))))))))

(add-watch stp-ins :stp-orders-watch execute-stp-orders)

(defn execute-lmt-orders
  [_ a _ new-state]
  (when-not (empty? new-state)
    (let [ins (first new-state)]
      (process-limit-instruction ins)
      (reset! lmt-ins
              (filter
                #(not= ins %)
                (distinct (flatten (conj new-state @lmt-ins))))))))

(add-watch lmt-ins :lmt-orders-watch execute-lmt-orders)

(defn process-instructions
  [ins]
  (reset! mkt-ins (distinct (flatten (conj (u/mkt-instructions ins) @mkt-ins))))
  (reset! stp-ins (distinct (flatten (conj (u/stp-instructions ins) @stp-ins))))
  (reset! lmt-ins (distinct (flatten (conj (u/lmt-instructions ins) @lmt-ins)))))
