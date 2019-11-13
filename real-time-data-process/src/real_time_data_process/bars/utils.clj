(ns real-time-data-process.bars.utils
  (:require
    ;; Vendor
    [clj-time.format :as f]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; Local
    [real-time-data-process.database.pull-from-db :as db-pull]
    [real-time-data-process.database.push-to-db :as db-push]
    [ib-client.client :as ib-client]
    [ib-client.utils :as ib-utils]
    [ib-client.time :as ib-time]))

(defn create-contract
  [ticker conid]
  (ib-client/create-contract {:symbol ticker
                              :sec-type "STK"
                              :conid conid
                              :exchange "SMART"
                              :currency "USD"}))

(defn first-rlt-bar
  [new-rlt-bar]
  {:date (c/to-timestamp (* (:time new-rlt-bar) 1000))
   :open (:open new-rlt-bar)
   :high (:high new-rlt-bar)
   :low (:low new-rlt-bar)
   :close (:close new-rlt-bar)
   :count (:count new-rlt-bar)
   :volume (:volume new-rlt-bar)
   :has_gaps nil})

(defn modify-rlt-bar
  [rlt-bar new-rlt-bar]
  {:date (c/to-timestamp (* (- (:time new-rlt-bar) 55) 1000))
   :open (:open rlt-bar)
   :high (if (> (:high new-rlt-bar)
                (:high rlt-bar))
           (:high new-rlt-bar)
           (:high rlt-bar))
   :low (if (< (:low new-rlt-bar)
               (:low rlt-bar))
          (:low new-rlt-bar)
          (:low rlt-bar))
   :close (:close new-rlt-bar)
   :volume (+ (:volume new-rlt-bar)
              (:volume rlt-bar))
   :count (+ (:count new-rlt-bar)
             (:count rlt-bar))
   :has_gaps nil})

(defn stamp-to-datetime
  [t]
  (c/from-long t))

(defn minute-beginning?
  [t]
  (= 0 (t/second (c/from-long (* t 1000)))))

(defn minute-ending?
  [t]
  (= 55 (t/second (c/from-long (* t 1000)))))

(defn fifth-minute?
  [t]
  (= (rem (t/minute (c/from-sql-time t)) 5) 0))

(defn day-open-bar
  [hist-data]
  (first
    (filter
      (fn [bar]
        (= (ib-utils/date-time-now
             (ib-time/ny-time-in-local-tz
               9 30 (t/time-zone-for-id "UTC") (t/now)))
           (:date bar)))
      hist-data)))

(defn previous-day-close-bar
  [hist-data]
  (first
    (filter
      (fn [bar]
        (let [datetime (f/parse
                         (f/formatter "yyyyMMdd  HH:mm:ss")
                         (:date bar))
              hour (t/hour datetime)
              minute (t/minute datetime)
              day (t/minus datetime (t/hours hour) (t/minutes minute))]
          (= (ib-utils/date-time-now
               (ib-time/ny-time-in-local-tz 15 59 (t/time-zone-for-id "UTC") day))
             (:date bar))))
      hist-data)))

(defn pull-and-write-minute-bar
  [ticker column-name time-frame]
  (let [db-rows (db-pull/pull-n-minute-bars ticker 2 time-frame "*" "desc")
        value ((keyword column-name) (last db-rows))
        date (:date (first db-rows))]
    (db-push/write-value-to-db value column-name ticker date time-frame)))
