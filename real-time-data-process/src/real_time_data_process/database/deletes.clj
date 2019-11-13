(ns real-time-data-process.database.deletes
  (:require
   ;; vendor
   [clojure.java.jdbc :as j]
   [honeysql.core :as sql]
   ;; local
   [db.core :as db]))

(defn delete-today-created-bars
  [ticker date]
  (j/execute! (db/get-ds)
    (sql/format {:delete-from :historical_data
                 :where [:and
                         [:= :ticker ticker]
                         [:>= :date date]]})))

(defn clean-historical-minute-data
  [ticker]
  (j/execute! (db/get-ds)
    (sql/format {:delete-from :historical_minute_data
                 :where [:= :ticker ticker]})))
