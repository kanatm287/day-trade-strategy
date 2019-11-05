(ns historical-data-load.day-trade.utils
  (:require
    ;; Vendor
    [clojure.set :as clj-set]
    [clj-time.coerce :as c]
    [clj-time.format :as f]
    [clj-time.core :as t]
    ;; Local
    [historical-data-load.database.pull-from-db :as db-pull]
    [historical-data-load.database.push-to-db :as db-push]
    [historical-data-load.database.deletes :as db-del]
    [ib-client.utils :as ib-utils]
    [log.core :as log]))

(defn remove-non-full-days
  [hist-data full-day-bars ticker]
  (let [non-full-days
        (remove nil?
          (flatten
            (map :date
              (remove #{}
                      (clj-set/difference
                        (set hist-data) (set full-day-bars))))))]
    (when-not (empty? non-full-days)
      (let [daily-dates
            (distinct
              (reduce (fn [coll x]
                        (conj coll (.substring x 0, 8)))
                      []
                      (flatten non-full-days)))]
        (doseq [date daily-dates]
          (when date
            (log/info
              "day-trade"
              "historical_data_load data"
              (str "add non full date " date " historical_data_load data for ticker "
                   ticker))
            (let [dates (db-pull/pull-non-full-holidays ticker)]
              (when-not (some #(.isEqual (f/parse
                                          (f/formatter "yyyyMMdd")
                                          date)
                                  %)
                               dates)
                (db-push/add-non-full-day ticker date)))
            (log/info
              "day-trade"
              "historical_data_load data"
              (str "remove non full date " date " historical_data_load data for ticker "
                   ticker))
            (db-del/delete-non-full-day date ticker)))))))

(defn periods
  [days]
  (if days (int (Math/floor (/ days 30))) 24))

(defn generate-month-date-times
  [days]
  (let [months (periods days)]
    (if (= months 0)
      [(ib-utils/date-time-now (t/now))]
      (reduce (fn [coll value]
                (conj
                  coll
                  (ib-utils/date-time-now
                    (t/minus (t/now) (t/months value)))))
              [(ib-utils/date-time-now (t/now))]
              (take months (iterate inc 1))))))

(defn last-load-date?
  [ticker]
  (let [last-load-date
        (:date
          (db-pull/pull-historical-last-data ticker "five minute"))]
    (if last-load-date
      (if (t/before? (c/from-sql-time last-load-date)
                     (t/minus (t/now) (t/days 30)))
        true false)
      true)))

(defn last-process-date?
  [ticker]
  (let [last-process-date
        (:prev_day_close
          (db-pull/pull-historical-last-data ticker "five minute"))]
    (if last-process-date true false)))

(defn sync-hist-and-test-data
  [ticker time-frame]
  (let [hist-start-date
          (:date
            (first
              (db-pull/pull-n-bars
                ticker 1 "five minute" "date" "asc")))
        hist-last-date
          (:date
            (first
              (db-pull/pull-n-bars
                ticker 1 "five minute" "date" "desc")))]
    (db-del/synchronize-historical-and-test-data
      ticker hist-start-date hist-last-date time-frame)))

(defn sync-hist-and-temp-data
  [ticker]
  (db-del/delete-two-year-excess-historical-daily-data ticker)
  (db-del/clean-hist-temp-data ticker)
  (db-push/copy-hist-to-temp ticker))

(defn all-tickers
  []
  (dedupe
    (sort
      (flatten
        (conj (db-pull/pull-day-trade-tickers)
              (db-pull/pull-five-minute-data-tickers))))))
