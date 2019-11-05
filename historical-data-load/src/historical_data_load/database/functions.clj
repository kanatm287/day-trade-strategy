(ns historical-data-load.database.functions
  (:require
    ;; vendor
    [clojure.java.jdbc :as j]
    [honeysql.core :as sql]
    [clj-time.format :as f]
    [clj-time.coerce :as c]
    ;; local
    [db.core :as db]))

(defn historical-data-writing-function
  [ticker {:keys [date open high low close volume count has_gaps]} time-frame]
  (when date
    (let [query
          (sql/format {:select [(sql/call :historical_bar_write
                                          :?date
                                          :?open
                                          :?high
                                          :?low
                                          :?close
                                          :?volume
                                          :?count
                                          :?has_gaps
                                          :?ticker
                                          :?table_name)]}
                      :params {:date (cond
                                       (= time-frame "5 mins")
                                       (if (string? date)
                                         (c/to-timestamp
                                          (f/parse
                                           (f/formatter "yyyyMMdd  HH:mm:ss")
                                           date))
                                         date)

                                       (= time-frame "1 day")
                                       (c/to-timestamp
                                        (f/parse
                                         (f/formatter "yyyyMMdd")
                                         date))

                                       (= time-frame "1 min")
                                       (if (string? date)
                                         (c/to-timestamp
                                          (f/parse
                                           (f/formatter "yyyyMMdd  HH:mm:ss")
                                           date))
                                         date))
                               :open open
                               :high high
                               :low low
                               :close close
                               :volume (int volume)
                               :count (int count)
                               :has_gaps (boolean has_gaps)
                               :ticker ticker
                               :table_name time-frame})]
      (j/query (db/get-ds) query))))

(defn test-data-writing-function
  [ticker {:keys [date open high low close volume count has_gaps]} time-frame]
  (when date
    (let [query
          (sql/format {:select [(sql/call :test_bar_write
                                          :?date
                                          :?open
                                          :?high
                                          :?low
                                          :?close
                                          :?volume
                                          :?count
                                          :?has_gaps
                                          :?ticker
                                          :?table_name)]}
                      :params {:date (cond
                                       (= time-frame "5 mins")
                                       (if (string? date)
                                         (c/to-timestamp
                                           (f/parse
                                             (f/formatter "yyyyMMdd  HH:mm:ss")
                                             date))
                                         date)

                                       (= time-frame "1 day")
                                       (c/to-timestamp
                                         (f/parse
                                           (f/formatter "yyyyMMdd")
                                           date))

                                       (= time-frame "1 min")
                                       (if (string? date)
                                         (c/to-timestamp
                                           (f/parse
                                             (f/formatter "yyyyMMdd  HH:mm:ss")
                                             date))
                                         date))
                               :open open
                               :high high
                               :low low
                               :close close
                               :volume (int volume)
                               :count (int count)
                               :has_gaps (boolean has_gaps)
                               :ticker ticker
                               :table_name time-frame})]
      (j/query (db/get-ds) query))))

(defn get-minute-dates
  [ticker]
  (j/query (db/get-ds)
           (sql/format {:select
                        [(sql/call :get_daily_dates_from_minute_timestamps
                                   :?ticker)]}
                       :params {:ticker ticker})
           {:row-fn :get_daily_dates_from_minute_timestamps}))

(defn get-five-minute-dates
  [ticker]
  (j/query (db/get-ds)
           (sql/format {:select
                        [(sql/call :get_daily_dates_from_five_minute_timestamps
                                   :?ticker)]}
                       :params {:ticker ticker})
           {:row-fn :get_daily_dates_from_five_minute_timestamps}))
