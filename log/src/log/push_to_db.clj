(ns log.push-to-db
  (:require
   ;; vendor
   [clojure.java.jdbc :as j]
   [honeysql.core :as sql]
   [clj-time.coerce :as c]
   [clj-time.core :as t]))
   ;; local

(defn write-log
  [strategy-id summary error-info db-spec]
  (j/execute! db-spec
    (sql/format
      {:insert-into :logs
       :values [{:date (c/to-timestamp (t/now))
                 :strategy_id strategy-id
                 :summary summary
                 :error_info error-info}]})))

(defn exec-log
  [strategy-id summary error-info db-spec]
  (j/execute! db-spec
    (sql/format
      {:insert-into :logs_execution
       :values [{:date (c/to-timestamp (t/now))
                 :strategy_id strategy-id
                 :summary summary
                 :error_info error-info}]})))

(defn manage-log
  [strategy-id summary error-info db-spec]
  (j/execute! db-spec
    (sql/format
      {:insert-into :logs_orders
       :values [{:date (c/to-timestamp (t/now))
                 :strategy_id strategy-id
                 :summary summary
                 :error_info error-info}]})))

(defn server-log
  [strategy-id summary error-info db-spec]
  (j/execute! db-spec
              (sql/format
                {:insert-into :logs_server
                 :values [{:date (c/to-timestamp (t/now))
                           :strategy_id strategy-id
                           :summary summary
                           :error_info error-info}]})))