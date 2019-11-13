(ns strategy-server.process_orders
  (:require
  ;;local
    [strategy-server.database.push-to-db :as db-push]
    [strategy-server.database.pull-from-db :as db-pull]
    [log.core :as log]
    [strategy-server.day-trade.day-trade :as day-trade]))

(defn process-filled-instruction
  [ins-id status]
  (let [ins (db-pull/pull-instruction-by-instruction-id ins-id)
        strategy (:strategy_id ins)
        id (:cll_int_id ins)
        oca-group (:oca_group ins)
        fill-price (:fill_price ins)
        account (:account ins)]
    (when
      (and (= strategy "day_trade") (= account "DU626968"))
      (db-push/update-day-trade-orders id oca-group fill-price))))

(defn update-commission-details
  [commission]
  (db-push/write-commission-to-db commission))

(defn insert-execution-details
  [order-id contract execution]
  (log/server-info "server" "execution" (str  (:account execution) " " order-id))
  (let [ins (db-pull/pull-instruction-for-order-id
              (:account execution) order-id)]
    (db-push/write-execution-to-db order-id contract execution ins)))

(defn set-order-status
  [instruction-id fill-price status]
  (db-push/update-instruction-status instruction-id fill-price status)
  (when
    (= status "filled")
    (process-filled-instruction instruction-id status)))

(defn set-executed-order-ib-id
  [instruction-id ib-id]
  (db-push/update-instruction-ib-id instruction-id ib-id))

(defn check-day-trade-quantity
  [ib-account-id position quantity fill-price]
  (let [fill-ins (db-pull/pull-filled-market-instructions
                   ib-account-id (:symbol position) fill-price)]
    (when fill-ins
      (when-not (and (= quantity (:quantity fill-ins))
                     (< quantity (:quantity fill-ins)))
        (let [fix-quantity (- (Math/abs quantity) (:quantity fill-ins))]
          (day-trade/create-fix-quantity-instruction
            ib-account-id fill-ins fix-quantity))))))
