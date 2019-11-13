(ns strategy-server.instructions
  (:require
   ;; local
    [strategy-server.day-trade.day-trade :as day-trade]
    [strategy-server.database.pull-from-db :as db-pull]))

(defn get-day-trade-instructions
  [ib-account-id balance]
  (day-trade/create-instruction ib-account-id balance)
  (db-pull/pull-instructions-for-account "day-trade" ib-account-id))
