(ns financial-data-load.database.deletes
  (:require
    ;; vendor
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    ;; local
    [db.core :as db]))

(defn remove-from-estimates-data
  [ticker]
  (j/execute! (db/get-ds)
              (sql/format {:delete-from :financial_data
                           :where [:= :ticker ticker]})))

(defn remove-nil-estimates
  []
  (j/execute! (db/get-ds)
              (sql/format {:delete-from :financial_data
                           :where [:= :estimates nil]})))
