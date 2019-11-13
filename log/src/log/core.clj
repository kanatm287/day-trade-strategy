(ns log.core
  (:require
    ;; Vendor
    ;;local
    [log.push-to-db :as db-push])

  (:gen-class))

(def db-spec (atom {}))

(defn init-db
  [d]
  (reset! db-spec d))

(defn info
  [strategy-id summary error-info]
  (db-push/write-log strategy-id summary error-info @db-spec))

(defn execute-order
  [strategy-id summary error-info]
  (db-push/exec-log strategy-id summary error-info @db-spec))

(defn manage-order
  [strategy-id summary error-info]
  (db-push/manage-log strategy-id summary error-info @db-spec))

(defn server-info
  [strategy-id summary error-info]
  (db-push/server-log strategy-id summary error-info @db-spec))
