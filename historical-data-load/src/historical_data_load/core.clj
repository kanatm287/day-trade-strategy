(ns historical-data-load.core
  (:require
    ;;vendor
    ;;local
    [historical-data-load.day-trade.historical :as day-trade]
    [historical-data-load.trade-params :as trade-params]
    [historical-data-load.database.deletes :as db-del]
    [historical-data-load.handlers :as handlers]
    [historical-data-load.load :as load]
    [ib-client.core :as ib-client]
    [log.core :as log]
    [db.core :as db])

  (:gen-class))

(defn prop
  [s]
  (System/getProperty s))

(defn ib-connect
  []
  (ib-client/start
   (Integer/parseInt (prop "ib.clientId"))
   {:error handlers/log-error}
   (Integer/parseInt (prop "ib.port"))))

(defn -main
  [& args]
  (db/init-database!)
  (log/init-db (db/get-ds))
  (let [api (ib-connect)]
    (db-del/delete-all-hist-data)
    (load/core api)
    (trade-params/core)
    (day-trade/load-data api)
    (day-trade/process-data)
    (ib-client/stop api)
    (System/exit 0)))
