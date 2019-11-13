(ns thin-client.exec-utils
  (:require
    ;; local
    [ib-client.wrapper :as wrapper]
    [ib-client.client :as client]
    [clj-time.core :as t]
    [thin-client.http-request :as reqs]))

(def api-atom (atom nil))

(def ^:private exec-time (atom nil))

(def ^:private orders-atom
       "Atom contains a map of orders data. Each element keyed with order-id
       for example:

       {12312 {:id: 12312
               :status :new/:submitted/:filled
               :quantity 65
               :strategy :carma/:simplicity
               :contract-params {...}
               :mkt-data {...}
               :action \"BTO\"/\"STO\"
               :db-status :new/:submitted/:filled}
        43242 {...}}."
       (atom {}))

(defn exists?
  "Returns true when order exists in orders-atom."
  [id]
  (contains? @orders-atom id))

(defn- update-atom
  "Apply update-fn to orders-atom."
  [update-fn]
  (swap! orders-atom update-fn))

(defn add
  "Add new order, or replace existing."
  [order]
  (update-atom #(assoc % (:id order) order)))

(defn set-status
  [id status]
  (when (exists? id)
    (update-atom #(assoc-in % [id :status] status))))

(defn set-fill-price
  [id fill-price]
  (when (exists? id)
    (update-atom #(assoc-in % [id :fill-price] fill-price))))

(defn sync-db-status
  "Copy value from :status to :db-status.
  This function must be called when order state is written to database."
  [id]
  (when (exists? id)
    (update-atom #(assoc-in % [id :db-status] (get-in % [id :status])))))

(defn get-order
  "Get order data by order id."
  [id]
  (get @orders-atom id))

(defn filled?
  "Returns true if order status equals to :filled"
  [id]
  (= :filled (:status (get-order id))))

(defn cancelled?
  "Returns true if order status equals to :cancelled"
  [id]
  (= :cancelled (:status (get-order id))))

(defn get-all-orders
  "Get list of all current orders."
  []
  (vals @orders-atom))

(defn status-updated?
  [order]
  (not= (:status order) (:db-status order)))

(defn remove-order
  "Remove order from orders-atom after it was filled and synced to database."
  [id]
  (update-atom #(dissoc % id)))

(defn start
  [api]
  (reset! api-atom api))

(defn stop
  []
  (reset! api-atom nil))

(defn- execute-new-order
  [_ a _ new-state]
  (let [orders (filter (fn [[k v]] (= (:status v) "new")) new-state)]
    (when orders
      (let [order-ids (keys orders)
            order-params (vals orders)]
        (doseq [order-id order-ids]
          (set-status order-id "working"))
        (doall
          (pmap
            (fn [order]
              (loop []
                (let [api @api-atom
                      contract (:contract order)
                      last-trade-second-passed
                      (if @exec-time
                        (t/before? @exec-time
                                   (t/minus (t/now) (t/seconds 1)))
                        true)]
                  (if last-trade-second-passed
                    (do
                      (client/place-contract-order
                        api contract (:order order) (:id order))
                      (reset! exec-time (t/now)))
                    (recur)))))
            order-params))))))

(add-watch orders-atom :orders-watch execute-new-order)

(defn send-order-statuses
  [_ a _ new-state]
  (let [updated-orders (->> (vals new-state)
                            (filter status-updated?)
                            (map #(select-keys % [:id
                                                  :instruction-id
                                                  :status
                                                  :fill-price])))
        result (when (seq updated-orders)
                 (reqs/update-order-statuses updated-orders))]
    (when result
      (doseq [{id :id} updated-orders]
        (sync-db-status id)
        (when (or (filled? id)
                  (cancelled? id))
          (remove-order id))))))

(add-watch orders-atom :status-watch send-order-statuses)