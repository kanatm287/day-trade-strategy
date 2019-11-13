(ns back-test.core
  (:require
    ;; Vendor
    ;; Local
    [back-test.database.pull-from-db :as db-pull]
    [back-test.database.push-to-db :as db-push]
    [back-test.scripts :as scripts]
    [back-test.utils :as u]
    [log.core :as log]
    [db.core :as db])
  (:import [fiveMinute Core])

  (:gen-class))

(def strategies ["trend" "correction"])

(defn run-machine-learning
  [ticker]
  (do
    (scripts/prepare-historical-data ticker)
    (scripts/classify-data ticker)
    (scripts/predict-five-minute-back-test-data ticker)))

(defn set-minute-performance
  [ticker action]
  (doseq [strategy strategies]
    (let [multiplier (if (= action "buy") 1 -1)
          spy
          (db-pull/pull-back-test-return
            ticker "minute" strategy action "spy")
          algo
          (db-pull/pull-back-test-return
            ticker "minute" strategy action "algo")
          beta
          (Math/abs
            (db-pull/pull-back-test-return
              ticker "minute" strategy action "beta"))
          max-drawdown
          (Math/abs
            (db-pull/pull-back-test-return
              ticker "minute" strategy action "max_drawdown"))]
      (when (and spy algo max-drawdown beta)
        (if (and
              (> algo max-drawdown)
              (> algo (* 0.5 (* spy multiplier)))
              (< beta 0.3))
          (db-push/update-test-data
            ticker "minute" true "performance" strategy action)
          (db-push/update-test-data
            ticker "minute" false "performance" strategy action))))))

(defn back-test-minute
  [action]
  (loop [ticker (db-pull/pull-minute-nil-ticker action)]
    (let [historical-data (Core.)]
      (when ticker
        (doseq [strategy strategies]
            (when (u/check-if-error? ticker)
              (if (u/get-portfolio-value ticker "minute" strategy action "")
                (when (u/last-test-date? ticker strategy action)
                  (scripts/zipline-back-test-minute ticker action strategy " "))
                (scripts/zipline-back-test-minute ticker action strategy " "))))
        (when (u/check-if-error? ticker)
          (.prepareData historical-data ticker))
        (doseq [strategy strategies]
          (when (u/check-if-error? ticker)
            (if (u/get-portfolio-value ticker "minute" strategy action "_benchmark")
              (when (u/last-test-date? ticker strategy action)
                (scripts/zipline-back-test-minute
                  ticker action strategy "use benchmark correlation"))
              (scripts/zipline-back-test-minute
                ticker action strategy "use benchmark correlation"))))
        (when (u/check-if-error? ticker)
          ;(run-machine-learning ticker) ; python scikit-learn
          (.preProcessData historical-data ticker)
          (.classifyData historical-data ticker)
          (.predictData historical-data ticker))
        (doseq [strategy strategies]
          (when (u/check-if-error? ticker)
            (if (u/get-portfolio-value ticker "minute" strategy action "_ml")
              (when (u/last-test-date? ticker strategy action)
                (scripts/zipline-back-test-minute
                  ticker action strategy "use machine learning"))
              (scripts/zipline-back-test-minute
                ticker action strategy "use machine learning")))))
      (when (u/check-if-error? ticker)
        (scripts/zipline-back-test-minute ticker "combined" action " ")
        (let [trend
              (db-pull/pull-back-test-return
                ticker "minute" "trend" action "combined")
              correction
              (db-pull/pull-back-test-return
                ticker "minute" "correction" action "combined")]
          (when (and trend correction)
            (set-minute-performance ticker action))))
      (.stopSparkSession historical-data))
    (recur (db-pull/pull-minute-nil-ticker action))))

(defn -main
  [& args]
  (db/init-database!)
  (log/init-db (db/get-ds))
  (loop []
    (back-test-minute "buy")
    (back-test-minute "sell")
    (Thread/sleep 1800000)
    (recur)))
