(ns back-test.database.push-to-db
  (:require
    ;; vendor
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    ;; local
    [db.core :as db]))

(defn update-test-data
  [ticker time-frame value column strategy action]
  (j/execute! (db/get-ds)
    (sql/format {:update :test_params
                 :set {(keyword column) value}
                 :where [:and
                         [:= :symbol ticker]
                         [:= :time_frame time-frame]
                         [:= :strategy strategy]
                         [:= :action action]]})))

