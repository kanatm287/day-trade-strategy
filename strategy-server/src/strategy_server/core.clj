(ns strategy-server.core
  (:require
   ;;vendor
   [compojure.core :refer [routes POST GET PUT wrap-routes]]
   [org.httpkit.server :as server]
   [ring.util.http-response :as http]
   [ring.middleware.params :refer [wrap-params]]
   [ring.middleware.json :refer [wrap-json-body wrap-json-params
                                 wrap-json-response]]
   [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
   [overtone.at-at :as at]
   [clojure.string :as string]
   [clojure.java.io :as io]
   [clj-time.coerce :as c]
   [clj-time.core :as t]
    ;;local
   [strategy-server.coercions :refer [as-float as-int32]]
   [strategy-server.database.pull-from-db :as db-pull]
   [strategy-server.process_orders :as process]
   [strategy-server.instructions :as inst]
   [strategy-server.consts :as C]
   [ib-client.time :as ib-time]
   [log.core :as log]
   [db.core :as db])


  (:gen-class))

(def job-pool (at/mk-pool))

(defn- balance-and-permissions-error
  [ib-account-id balance strategy-id]
  (when-not (and (> balance C/MIN-BALANCE)
                 ((keyword (string/replace strategy-id #"-" "_"))
                  (db-pull/pull-permissions-for-account ib-account-id)))
    (http/not-found (str "No account permissions, or balance less than $"
                         C/MIN-BALANCE))))

(def my-api
  (-> (routes

       (POST "/error" {error-data :params}
         (http/ok
          {:error (log/server-info "error" "thin client" error-data)}))

       (POST "/execution" [order-id contract execution]
         (process/insert-execution-details order-id contract execution)
         (http/ok))

       (PUT "/commission" {comission-data :params}
         (process/update-commission-details comission-data)
         (http/ok))

       (PUT "/statuses" {status-data :params}
         (doseq [{:keys [instruction-id fill-price status]} (:data status-data)]
           (when (and instruction-id status)
             (log/server-info "server put" "set order status" (str instruction-id " " status))
             (process/set-order-status instruction-id fill-price status)))
         (http/ok))

       (PUT "/ib-id" [instruction-id ib-id]
         (log/server-info "server put" "set ib id for order" (str instruction-id " " ib-id))
         (process/set-executed-order-ib-id instruction-id ib-id)
         (http/ok))

       (GET "/order/:order-id{\\d+}"
            [order-id :<< as-int32
             ib-account-id]
         (log/server-info "server get" "get order ids" (str ib-account-id " " order-id))
         (let [data (db-pull/pull-instruction-for-order-id
                     ib-account-id order-id)]
           (when data
             (http/ok data))))

       (POST "/spot-positions"
             [ib-account-id
              positions]
         (doseq [{:keys [contract position traded-price]} positions]
           (when (and contract position traded-price)
             (process/check-day-trade-quantity
               ib-account-id contract position traded-price)))
         (http/ok))

       (POST "/instructions"
             [ib-account-id
              balance
              strategy-id
              positions]
         (println ib-account-id balance strategy-id positions)
         (or (balance-and-permissions-error ib-account-id balance strategy-id)
             (case strategy-id
               "day-trade"
               (http/ok
                (inst/get-day-trade-instructions ib-account-id balance))

               (http/not-found "Strategy not found")))))

      (wrap-json-response)
      (wrap-defaults api-defaults)
      (wrap-json-body)
      (wrap-json-params)))

(defonce server-instance (atom nil))

(defn stop-server []
  (when-not (nil? @server-instance)
    ;; graceful shutdown: wait 100ms or existing requests to be finished
    ;; :timeout is optional, when no timeout, stop immediately
    (@server-instance :timeout 100)
    (reset! server-instance nil)))

(defn shut-down
  []
  (log/server-info "server" "shutting down..." nil)
  (at/stop-and-reset-pool! job-pool :strategy :kill)
  (stop-server)
  (System/exit 0))

(defn -main
  [& args]
  (let [time-now (c/to-long (t/now))
        time-end (c/to-long (ib-time/ny-time-in-local-tz 16 05 nil nil))
        time-delta (- time-end time-now)]
    (db/init-database!)
    (log/init-db (db/get-ds))
    (reset! server-instance (server/run-server #'my-api {:port 7777}))
    (log/server-info "server" "running at port 7777" nil)
    (at/after time-delta #(shut-down) job-pool)))
