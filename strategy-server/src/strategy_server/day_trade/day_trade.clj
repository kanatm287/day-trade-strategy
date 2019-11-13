(ns strategy-server.day-trade.day-trade
  (:require
    ;; Vendor
    [clj-time.core :as t]
    [clj-time.coerce :as c]
    ;;local
    [strategy-server.database.pull-from-db :as db-pull]
    [strategy-server.consts :refer [DAY-TRADING-BALANCE TRADES-PER-MONTH]]
    [strategy-server.database.push-to-db :as db-push]
    [log.core :as log]
    [strategy-server.day-trade.utils :as u]))

(defn process-open-stock-order
  [ib-account-id order quantity]
  (log/server-info
    "day trade" "process day trade open stock order" (:ticker order))
  (db-push/write-instruction-to-db
    {:date (c/to-timestamp (t/now))
     :account ib-account-id
     :order-status "new"
     :signal-id "open stock"
     :strategy-id "day trade"
     :cll-int-id (:ib_id order)
     :symbol (:ticker order)
     :sec-type "STK"
     :multiplier nil
     :currency "USD"
     :order-type "MKT"
     :buy-or-sell (:action order)
     :quantity quantity
     :transmit true
     :tif "DAY"
     :oca-group (:oca_group order)
     :conids (str (:conid order))}))

(defn process-close-stock-order
  [ib-account-id order quantity]
  (log/server-info "day trade"
                   "process day trade close stock order"
                   (:ticker order))
  (db-push/write-instruction-to-db
    {:date (c/to-timestamp (t/now))
     :account ib-account-id
     :order-status "new"
     :signal-id "close stock"
     :strategy-id "day-trade"
     :cll-int-id (:ib_id order)
     :symbol (:ticker order)
     :sec-type "STK"
     :multiplier nil
     :currency "USD"
     :order-type (if (= (:order_type order) "TAKE PROFIT") "LMT" "STP")
     :buy-or-sell (:action order)
     :limit-price (when (= (:order_type order) "TAKE PROFIT")
                    (:trailing_percent order))
     :stop-price (when (= (:order_type order) "STOP LOSS")
                   (:trailing_percent order))
     :quantity quantity
     :transmit true
     :tif "DAY"
     :oca-group (:oca_group order)
     :conids (str (:conid order))}))

(defn create-fix-quantity-instruction
  [ib-account-id ins quantity]
  (log/server-info "day trade"
                   "create fix quantity instruction"
                   (str "ticker "(:symbol ins) "account" ib-account-id))
  (db-push/write-instruction-to-db
    {:date (c/to-timestamp (t/now))
     :account ib-account-id
     :order-status "new"
     :signal-id "fix quantity"
     :strategy-id "day-trade"
     :cll-int-id nil
     :symbol (:symbol ins)
     :sec-type "STK"
     :multiplier nil
     :currency "USD"
     :order-type "MKT"
     :buy-or-sell (if (= (:buy_or_sell ins) "BUY") "SELL" "BUY")
     :limit-price nil
     :stop-price nil
     :quantity quantity
     :transmit true
     :tif "DAY"
     :oca-group (str (:oca_group ins) "_error")
     :conids (:conids ins)}))

(defn process-trade
  [ib-account-id balance order]
  (if (and (= (:order_type order) "MKT")
           (< (c/to-long (c/from-sql-time (:date order)))
              (c/to-long (t/minus (t/now) (t/seconds 30)))))
    (let [quantity (* (int (u/calculate-quantity balance order))
                      (:day_trade_multiplier
                        (db-pull/pull-permissions-for-account ib-account-id)))]
      (process-open-stock-order ib-account-id order quantity))
    (when-let [quantity (db-pull/pull-instruction-open-order-quantity
                            (:oca_group order) ib-account-id)]
      (process-close-stock-order ib-account-id order quantity))))

(defn create-instruction
  [ib-account-id balance]
  (let [tickers (db-pull/pull-tickers-from-day-trade-orders)]
    (when-not (empty? tickers)
      (doseq [ticker tickers]
        (let [orders (db-pull/pull-orders-from-day-trade-orders ticker)
              mkt-order (first (u/market-order orders))
              stop-order (first (u/stop-order orders))
              limit-order (first (u/limit-order orders))]
          (when mkt-order (process-trade ib-account-id balance mkt-order))
          (when stop-order (process-trade ib-account-id balance stop-order))
          (when limit-order (process-trade ib-account-id balance limit-order)))))))
