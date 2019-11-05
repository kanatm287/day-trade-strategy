(ns historical-data-load.day-trade.historical
  (:require
    ;; Vendor
    ;; Local
    [historical-data-load.utils :refer [get-contract-details-params]]
    [historical-data-load.utils :refer [test-entry-not-exists?]]
    [historical-data-load.day-trade.load-and-write-data :as load]
    [historical-data-load.database.pull-from-db :as db-pull]
    [historical-data-load.day-trade.fill-absent :as absent]
    [historical-data-load.database.push-to-db :as db-push]
    [historical-data-load.database.deletes :as db-del]
    [historical-data-load.day-trade.utils :as u]
    [historical-data-load.scripts :as scripts]
    [log.core :as log]))

(defonce actions ["buy" "sell"])

(defonce strategies ["trend" "correction"])

(defn load-historical-five-minute-data
  [api]
  (log/info "day-trade" "load 2 years historical_data_load data and write it to db" nil)
  (let [tickers (u/all-tickers)]
    (doall
      (pmap
        (fn [ticker]
          (when (u/last-load-date? ticker)
            (log/info
              "day-trade" "test data" (str "synchronize test data for " ticker))
            (u/sync-hist-and-temp-data ticker)
            (load/load-five-minute-data api ticker)
            (db-del/delete-two-year-excess-rows ticker) ; comment this line if you are running python process data scripts
            (db-push/set-volume-max-min ticker)))
        tickers))))

(defn load-test-minute-data
  [api]
  (log/info "day-trade" "load 2 years test data and write it to db" nil)
  (let [tickers (u/all-tickers)]
    (doall
      (pmap
        (fn [ticker]
          (log/info
            "day-trade" "test data" (str "synchronize test data for " ticker))
          (u/sync-hist-and-temp-data ticker)
          (load/load-minute-data api ticker))
        tickers))))

(defn load-benchmark-data
  [api]
  (let [benchmarks (db-pull/pull-benchmarks)]
    (doseq [benchmark benchmarks]
      (u/sync-hist-and-temp-data (:becnhmark benchmark))
      (load/load-five-minute-data api (:benchmark benchmark))
      (when-not (:conid benchmark)
        (db-push/set-benchmark-params
          benchmark
          (.conid (get-contract-details-params api benchmark)))))))

(defn process-historical-and-benchmark-data
  []
  (log/info "day-trade" "process 2 years historical_data_load data and write it to db" nil)
  (scripts/prepare-counter-data)
  (let [tickers (filter #(u/last-process-date? %)
                        (db-pull/pull-day-trade-tickers))
        benchmarks (db-pull/pull-benchmarks)]
    (doall
      (pmap
        (fn [benchmark]
          (scripts/prepare-benchmark-data
            (:benchmark benchmark) (:industry benchmark)))
        benchmarks))
    (doall
      (pmap
        (fn [ticker]
          (scripts/prepare-historical-data ticker)
          (db-del/delete-two-year-excess-rows ticker))
        tickers))))

(defn process-tickers
  []
  (doseq [action actions]
    (doseq [ticker (db-pull/pull-tickers (keyword (str "day_trade_" action)))]
      (when ticker
        (doseq [strategy strategies]
          (when-not (test-entry-not-exists? ticker "minute" strategy action)
            (log/info
              "day-trade" "test data"
              (str "add ticker "strategy " " action " to test data"))
            (db-push/add-ticker-to-test-data ticker "minute" strategy action)))))))

(defn load-data
  [api]
  (load-historical-five-minute-data api)
  (load-benchmark-data api)
  (load-test-minute-data api))

(defn process-data
  []
  ;uncomment process-historical-and-benchmark-data for python data processing
  ;(process-historical-and-benchmark-data)
  (absent/process-absent)
  (process-tickers))
