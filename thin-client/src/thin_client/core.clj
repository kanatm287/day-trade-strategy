(ns thin-client.core
  (:require
   ;;vendor
    [overtone.at-at :as at]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
   ;;local
    [ib-client.client :as ib-client]
    [ib-client.time :as ib-time]
    [ib-client.core :as client]
    [thin-client.strat.day-trade :as day-trade]
    [thin-client.handlers :as handlers]
    [thin-client.http-request :as reqs]
    [thin-client.exec-utils :as exec]
    [thin-client.utils :as u])

  (:gen-class)
  (:import (java.net ConnectException)))

(defonce job-pool (at/mk-pool))

(defn prop
  [s]
  (System/getProperty s))

(defn ib-connect
  []
  (client/start (Integer/parseInt (prop "ib.clientId"))
                {:error handlers/log-error
                 :order-status handlers/order-status-handler
                 :open-order handlers/open-order-handler
                 :position handlers/position-handler
                 :exec-details handlers/exec-details-handler
                 :commissions handlers/commissions-handler}
                (prop "ib.host")
                (Integer/parseInt (prop "ib.port"))))

(defn shut-down
  [api]
  (println "shutting down...")
  (at/stop-and-reset-pool! job-pool :strategy :kill)
  (exec/stop)
  (ib-client/stop api)
  (System/exit 0))

(defn create-day-trade-job
  [ib-account-ids accounts-balance]
  (fn []
    (println "run day_trade job")
    (doseq [ib-account-id ib-account-ids]
      (let [balance (get accounts-balance ib-account-id)
            instructions
            (reqs/request-day-trade-instructions ib-account-id balance)]
        (day-trade/process-instructions instructions)))))

(defn start-thin-client
  [api]
  (let [ib-account-ids (u/get-ib-account-ids api)
        balances (reduce (fn [b acc-id]
                           (assoc b acc-id (u/get-account-balance api acc-id)))
                         {} ib-account-ids)
        time-now (c/to-long (t/now))
        time-end (c/to-long (ib-time/ny-time-in-local-tz 16 05 nil nil))
        time-delta (- time-end time-now)]
    (exec/start api)
    (ib-client/request-open-orders api)
    (at/interspaced
     5000
     (create-day-trade-job ib-account-ids balances) job-pool)

    (at/after time-delta #(shut-down api) job-pool)))

(defn -main
  [& _]
  (try
    (start-thin-client (ib-connect))
    (catch ConnectException _
      (println "Could not connect to strategy server"))))

