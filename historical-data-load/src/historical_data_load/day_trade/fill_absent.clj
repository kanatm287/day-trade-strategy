(ns historical-data-load.day-trade.fill-absent
  (:require
    ;; Vendor
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; Local
    [historical-data-load.database.pull-from-db :as db-pull]
    [historical-data-load.database.push-to-db :as db-push]
    [historical-data-load.database.functions :as db-func]
    [historical-data-load.day-trade.utils :as u]
    [ib-client.time :as ib-time]
    [log.core :as log]))

(defn get-nearest-bar
  [date rows]
  (first
    (reduce
      (fn [[x delta] y]
        (let [end-date (c/from-sql-time (:date y))
              interval (t/in-minutes
                         (t/interval
                           (if (t/before? date end-date) date end-date)
                           (if (t/before? date end-date) end-date date)))]
          (if (< (Math/abs interval) delta) [y interval] [x delta])))
      [nil 390]
      rows)))

(defn generate-dates
  [rows start-date end-date]
  (->>
    (loop [acc []
           x (t/plus start-date (t/minutes 1))]
      (if (t/after? x end-date)
        acc
        (recur (conj acc x) (t/plus x (t/minutes 1)))))
    (reduce
      (fn [acc x]
        (if (some #(.isEqual x (c/from-sql-time (:date %))) rows)
          acc
          (conj
            acc
            (assoc (get-nearest-bar x rows)
              :date (c/to-sql-time (t/minus x (t/minutes 1)))))))
      [])))

(defn get-non-full-test-days
  [ticker]
  (let [holidays (db-pull/pull-non-full-holidays ticker)]
    (->> (db-pull/pull-test-dates ticker)
         (reduce
           (fn [acc date]
             (let [session-range (ib-time/generate-session-range date)]
               (if (db-pull/check-test-minute-dates? ticker session-range)
                 (conj acc date)
                 acc)))
           [])
         (reduce
           (fn [acc x]
             (if (some #(= x %) holidays)
               acc
               (conj acc x)))
           []))))

(defn fill-absent-bars
  [ticker]
  (loop [dates (get-non-full-test-days ticker)]
    (when-not (empty? dates)
      (doseq [date dates]
        (let [session-range (ib-time/generate-session-range date)
              test-rows
              (db-pull/pull-historical-data-by-date ticker session-range "minute")
              result-rows
              (generate-dates
                test-rows (first session-range) (last session-range))]
          (when (> (count result-rows) 10)
            (db-push/write-ticker-to-illiquid-list ticker))
          (doseq [row result-rows]
            (log/info
              "day-trade"
              "historical_data_load test data"
              (str "writing historical_data_load absent test data " row
                   " test minute data to db for ticker " ticker))
            (db-func/test-data-writing-function ticker row "1 min"))))
      (recur (get-non-full-test-days ticker)))))

(defn process-absent
  []
  (log/info "day-trade" "process absent minute data" nil)
  (let [tickers (sort (db-pull/pull-five-minute-data-tickers))]
    (doall
      (pmap
        (fn [ticker]
          (u/sync-hist-and-test-data ticker "minute")
          (fill-absent-bars ticker))
        tickers))))
