(ns historical-data-load.day-trade.load-and-write-data
  (:require
    ;; Vendor
    ;; Local
    [historical-data-load.database.pull-from-db :refer [pull-trade-params]]
    [historical-data-load.utils :refer [days-to-download create-contract]]
    [historical-data-load.database.functions :as db-func]
    [historical-data-load.day-trade.utils :as u]
    [ib-client.utils :as ib-utils]
    [log.core :as log]))

(defn full-days-only
  [ticker hist-data time-frame]
  (let [full-day-bars
        (->> hist-data
             (reduce (fn [acc bar]
                       (if (and bar (:date bar))
                         (let [k (.substring (:date bar) 0, 8)]
                           (if (get acc k)
                             (update acc k #(conj % bar))
                             (assoc acc
                               (.substring (:date bar) 0, 8)
                               [bar])))
                         acc))
                     {})
             (filter (fn [[k v]]
                       (= 78 (count v))))
             vals
             flatten)]
    (u/remove-non-full-days hist-data full-day-bars ticker)
    (doall
      (pmap
        (fn [bar]
          (db-func/historical-data-writing-function
            ticker bar time-frame))
        full-day-bars))))

(defn load-and-write-five-minute-historical-data
  [api ticker date-times]
  (doseq [date-time date-times]
    (let [hist-data (map :data
                         (ib-utils/request-ticker-historical-data
                           api
                           (create-contract
                             ticker
                             (:conid (pull-trade-params ticker)))
                           "5 mins"
                           "1 D"
                           date-time))]
      (log/info
        "day-trade"
        "historical_data_load data"
        (str "writing 5 minute historical_data_load data to db for ticker "
             ticker " missed date " date-time))
      (full-days-only ticker hist-data "5 mins"))))

(defn load-missed-five-minute-dates
  [api ticker]
  (loop []
    (let [missed-dates (db-func/get-five-minute-dates ticker)]
      (when-not (empty? missed-dates)
        (load-and-write-five-minute-historical-data api ticker missed-dates)
        (recur)))))

(defn load-five-minute-data
  [api ticker]
  (let [check-days (days-to-download ticker "five minute")
        days (if check-days (when (> check-days 0) "35 D") "35 D")]
    (when days
      (let [date-times (u/generate-month-date-times check-days)]
        (doseq [date-time date-times]
          (let [hist-data (map :data
                               (ib-utils/request-ticker-historical-data
                                 api
                                 (create-contract
                                   ticker
                                   (:conid (pull-trade-params ticker)))
                                 "5 mins"
                                 days
                                 date-time))]
            (log/info
              "day-trade"
              "historical_data_load data"
              (str "writing 5 minute historical_data_load data to db for ticker "
                   ticker))
            (full-days-only ticker hist-data "5 mins")))))
    (load-missed-five-minute-dates api ticker)))

(defn load-and-write-test-data
  [api ticker date-times]
  (doseq [date-time date-times]
    (let [hist-data
          (map :data
               (ib-utils/request-ticker-historical-data
                 api
                 (create-contract
                   ticker
                   (:conid (pull-trade-params ticker)))
                 "1 min"
                 "1 D"
                 date-time))]
      (when hist-data
        (log/info
          "day-trade"
          "historical_data_load test data"
          (str "writing historical_data_load test data " date-time
               " test minute data to db for ticker " ticker))
        (doall
          (pmap
            (fn [bar]
              (db-func/test-data-writing-function
                ticker bar "1 min"))
            hist-data))))))

(defn load-missed-minute-dates
  [api ticker]
  (loop [counter 100]
    (when (> counter 0)
      (let [missed-dates (db-func/get-minute-dates ticker)]
        (when-not (empty? missed-dates)
          (load-and-write-test-data api ticker missed-dates)
          (recur (dec counter)))))))

(defn load-minute-data
  [api ticker]
  (let [check-days (days-to-download ticker "minute")
        days (if check-days (when (> check-days 0) "30 D") "30 D")]
    (when days
      (let [date-times (u/generate-month-date-times check-days)]
        (doseq [date-time date-times]
          (let [hist-data
                (map :data
                     (ib-utils/request-ticker-historical-data
                       api
                       (create-contract
                         ticker
                         (:conid (pull-trade-params ticker)))
                       "1 min"
                       days
                       date-time))]
            (when hist-data
              (log/info
                "day-trade"
                "historical_data_load test data"
                (str "writing historical_data_load month test data " date-time
                     " test minute data to db for ticker " ticker))
              (doall
                (pmap
                  (fn [bar]
                    (db-func/test-data-writing-function
                      ticker bar "1 min"))
                  hist-data))))))
      (load-missed-minute-dates api ticker))))
