(ns back-test.utils
  (:require
    ;; vendor
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; local
    [back-test.database.pull-from-db :as db-pull]))

(defn last-test-date?
  [ticker strategy action]
  (let [last-test-date
        (db-pull/pull-last-test-date
          ticker
          (if (or (= strategy "reports")
                  (= strategy "eps-growth"))
            "daily"
            "minute")
          strategy
          action)]
    (if last-test-date
      (if (t/before? (c/from-sql-time last-test-date)
                     (t/minus (t/now) (t/days (if (or (= strategy "reports")
                                                      (= strategy "eps-growth"))
                                                180
                                                30))))
        true false)
      true)))

(defn get-portfolio-value
  [ticker time-frame strategy action additional]
  (db-pull/pull-back-test-value ticker time-frame strategy action additional))

(defn check-if-error?
  [ticker]
  (not
    (boolean
      (some #(= ticker %) (db-pull/pull-back-test-error-minute-tickers)))))
