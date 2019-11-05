(ns historical-data-load.load
  (:require
    ;; Vendor
    [clj-time.core :as t]
    ;; Local
    [historical-data-load.utils :refer [days-to-download create-contract]]
    [historical-data-load.database.pull-from-db :as db-pull]
    [historical-data-load.database.functions :as db-func]
    [ib-client.utils :as ib-utils]
    [log.core :as log]))

(defn load-daily-historical-data
  [api days last-date ticker]
  (log/info "day-trade"
            (str "load " days " daily historical_data_load data")
            (str "ticker " ticker))
  (let [hist-data (map :data (ib-utils/request-ticker-historical-data
                               api
                               (create-contract
                                 ticker
                                 (:conid (db-pull/pull-trade-params ticker)))
                               "1 day"
                               days
                               last-date))]
    hist-data))

(defn write-daily-historical-data
  [ticker hist-data]
  (doall
    (pmap
      (fn [bar]
        (db-func/historical-data-writing-function
          ticker bar "1 day"))
      hist-data)))

(defn load-two-year-historical-data
  [api ticker]
  (let [dates [(ib-utils/date-time-now (t/now))
               (ib-utils/date-time-now (t/minus (t/now) (t/years 1)))]]
    (doseq [date dates]
      (let [hist-data (load-daily-historical-data api "1 Y" date ticker)]
        (write-daily-historical-data ticker hist-data)))))

(defn load-daily-data
  [api]
  (doseq [ticker (remove nil?
                         (flatten
                           (conj (db-pull/pull-all-tickers)
                                 (map :benchmark (db-pull/pull-benchmarks)))))]
    (let [check-days (days-to-download ticker "daily")]
      (if check-days
        (write-daily-historical-data
          ticker
          (load-daily-historical-data
            api
            (str (+ check-days 1) " D")
            (ib-utils/date-time-now (t/now))
            ticker))
        (load-two-year-historical-data api ticker)))))

(defn core
  [api]
  (load-daily-data api))
