(ns financial-data-load.core
  (:require
    ;;vendor
    [clojure.set :as set]
    ;;local
    [financial-data-load.database.pull-from-db :as db-pull]
    [financial-data-load.database.push-to-db :as db-push]
    [financial-data-load.load-financial-data :as load]
    [financial-data-load.handlers :as handlers]
    [ib-client.core :as ib-client]
    [screener.core :as screener]
    [log.core :as log]
    [db.core :as db])

  (:gen-class))

(defn prop
  [s]
  (System/getProperty s))pr

(defn ib-connect
  []
  (ib-client/start
    (Integer/parseInt (prop "ib.clientId"))
    {:error handlers/log-error}
    (Integer/parseInt (prop "ib.port"))))

(defn set-tickers-to-db
  [tickers column]
  (doall
    (pmap
      (fn [ticker]
        (db-push/write-ticker-to-db ticker))
      (set/difference
        (set tickers)
        (set (map :ticker (db-pull/pull-existed-estimates-tickers))))))
  (db-push/set-column-true tickers column))

(defn -main
  [& args]
  (db/init-database!)
  (log/init-db (db/get-ds))
  (set-tickers-to-db (screener/day-trade) :day_trade)
  (let [api (ib-connect)]
    (load/load-estimates-data api)
    (ib-client/stop api))
  (System/exit 0))
