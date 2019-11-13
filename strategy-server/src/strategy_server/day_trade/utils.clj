(ns strategy-server.day-trade.utils
  (:require
    ;; Vendor
    ;;local
    [strategy-server.consts :refer [DAY-TRADING-BALANCE TRADES-PER-MONTH]]
    [strategy-server.database.pull-from-db :as db-pull]
    [clojure.string :as s]))

(defn market-order
  [orders]
  (filter #(when (= (:order_type %) "MKT") %) orders))

(defn stop-order
  [orders]
  (filter #(when (= (:order_type %) "STOP LOSS") %) orders))

(defn limit-order
  [orders]
  (filter #(when (= (:order_type %) "TAKE PROFIT") %) orders))

(defn calculate-quantity
  [balance market-order]
  (let [dt-balance (/ (* balance DAY-TRADING-BALANCE) TRADES-PER-MONTH)
        action (:action market-order)
        last-close (:last_close market-order)
        trail-price (db-pull/pull-technical-param
                      (:ticker market-order)
                      (:signal_id market-order)
                      (s/lower-case action)
                      "trail_percent")]
    (if (= action "BUY")
      (/ dt-balance (- last-close (* last-close (if trail-price trail-price 0.98))))
      (/ dt-balance (- (* last-close (if trail-price (+ 1 (- 1 trail-price)) 1.02)) last-close)))))
