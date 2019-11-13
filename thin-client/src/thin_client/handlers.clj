(ns thin-client.handlers
  (:require
   ;;  Vendor
   ;;  Local
   [thin-client.utils :refer [precision-round]]
   [ib-client.wrapper :as wrapper]
   [thin-client.http-request :as reqs]
   [thin-client.exec-utils :as exec]
   [clojure.core.async :as async]))

(defn log-error
  [err]
  (let [{:keys [error-code error-msg]} (:data err)]
    (when error-code
      (println (str "Error(code " error-code "): " error-msg)))))


(defn update-atom-db
  [id fill-price status]
  (exec/set-status id status)
  (exec/set-fill-price id fill-price))

(defn open-order-handler
  [resp]
  (when (wrapper/success-response? resp)
    (let [{:keys [req-id contract order status]} (:data resp)]
      (when-not (exec/exists? req-id)
        (let [order-status (cond (= status "Filled") :filled
                                 (= status "Submitted") :submitted
                                 (= status "PreSubmitted") :pre-submitted
                                 (= status "PendingSubmit") :pending-submit
                                 (= status "PendingCancel") :pending-cancel
                                 (= status "ApiCancelled") :api-cancelled
                                 (= status "Cancelled") :cancelled
                                 (= status "Inactive") :inactive)]
          (when-let [instruction (reqs/request-order-instruction
                                  (.account order) req-id)]
            (exec/add {:id req-id
                       :instruction-id (:instruction_id instruction)
                       :status order-status
                       :signal-id (:signal_id instruction)
                       :strategy-id (:strategy_id instruction)
                       :contract contract
                       :order {:action (.getApiString (.action order))
                               :order-type (.getApiString (.orderType order))
                               :stop-price (.auxPrice order)
                               :limit-price (.lmtPrice order)
                               :trailing-percent (.trailingPercent order)
                               :transmit (.transmit order)
                               :total-quantity (.totalQuantity order)
                               :oca-group (.ocaGroup order)
                               :account (.account order)}
                       :time-in-force (.getApiString (.tif order))
                       :db-status (:order_status instruction)})))))))

(defn order-status-handler
  [resp]
  (when (wrapper/success-response? resp)
    (let [{:keys [req-id status remaining avg-fill-price]} (:data resp)]
      (update-atom-db
       req-id
       (when avg-fill-price (precision-round 2 avg-fill-price))
       (cond (and (= remaining 0.0) (= status "Filled")) :filled
             (and (= remaining 0.0) (= status "Submitted")) :filled
             (= status "PreSubmitted") :pre-submitted
             (= status "PendingSubmit") :pending-submit
             (= status "PendingCancel") :pending-cancel
             (= status "ApiCancelled") :api-cancelled
             (= status "Cancelled") :cancelled
             (= status "Inactive") :inactive)))))

(defn position-handler
  [resp]
  (when (wrapper/success-response? resp)
    (let [{:keys [account contract pos avgCost]} (:data resp)])))

(defn exec-details-handler
  [resp]
  (when (wrapper/success-response? resp)
    (let [{:keys [contract execution]} (:data resp)]
      (async/thread
        (reqs/insert-execution
         {:order-id (.orderId execution)
          :contract {:symbol (.symbol contract)
                     :sec-type (.getApiString (.secType contract))
                     :expire (.lastTradeDateOrContractMonth contract)
                     :strike (.strike contract)
                     :right (.getApiString (.right contract))
                     :exchange (.exchange contract)
                     :currency (.currency contract)
                     :combo-legs (.comboLegsDescrip contract)}
          :execution {:exec-id (.execId execution)
                      :account (.acctNumber execution)
                      :side (.side execution)
                      :quantity (.shares execution)
                      :price (.price execution)
                      :cum-quant (.cumQty execution)
                      :avg-price (.avgPrice execution)
                      :order-reference (.orderRef execution)}})))))

(defn commissions-handler
  [resp]
  (when (wrapper/success-response? resp)
    (let [{:keys [commissions]} (:data resp)
          realized-profit-loss (when (not= Double/MAX_VALUE
                                           (.m_realizedPNL commissions))
                                 (.m_realizedPNL commissions))
          yield (when (not= Double/MAX_VALUE (.m_yield commissions))
                  (.m_yield commissions))]
      (async/thread
        (reqs/update-commission
         {:exec-id (.m_execId commissions)
          :commission (.m_commission commissions)
          :currency (.m_currency commissions)
          :realized-profit-loss realized-profit-loss
          :yield yield
          :yield-date (.m_yieldRedemptionDate commissions)})))))
