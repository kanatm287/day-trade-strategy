(ns historical-data-load.utils
  (:require
    ;; vendor
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; local
    [historical-data-load.database.pull-from-db :as db-pull]
    [ib-client.client :as ib-client]))

(defn create-contract
  [ticker conid]
  (ib-client/create-contract {:symbol ticker
                              :sec-type "STK"
                              :conid conid
                              :exchange "SMART"
                              :currency "USD"}))

(defn get-contract-details-params
  [api ticker]
  (:contract-details
    (:data
      (ib-client/request-contract
        api
        {:symbol ticker
         :sec-type "STK"
         :exchange "SMART"
         :currency "USD"}))))

(defn test-entry-not-exists?
  [ticker time-frame strategy action]
  (boolean (db-pull/pull-test-params ticker time-frame strategy action)))

(defn days-to-download
  [ticker time-frame]
  (if-not (db-pull/table-contains-ticker? ticker time-frame)
    (-> ticker
        (db-pull/pull-historical-last-data time-frame)
        :date
        c/from-sql-date
        (t/interval (t/now))
        t/in-days)))
