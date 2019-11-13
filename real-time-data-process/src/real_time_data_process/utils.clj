(ns real-time-data-process.utils
  (:require
    ;; vendor
    [clojure.string :as s]
    ;; local
    [real-time-data-process.database.pull-from-db :as db-pull]))

(def technical-params-map (atom nil))
(def basic-bar-params-map (atom nil))

(defn get-back-test-param
  [ticker strategy action column]
  (db-pull/pull-test-params
    ticker
    strategy
    (s/lower-case action)
    column))

(defn create-technical-params
  [params action tickers]
  (reduce
    (fn [result-map ticker]
      (conj
        result-map
        {(keyword ticker)
         {:trend (get-back-test-param ticker "trend" action "technical_params")
          :correction
          (get-back-test-param ticker "correction" action "technical_params")}}))
    params
    tickers))

(defn create-technical-params-map
  [buy-tickers sell-tickers]
  (create-technical-params
    (create-technical-params {} "BUY" (map :ticker buy-tickers))
    "SELL"
    (map :ticker sell-tickers)))

(defn precision-round
  "Round a double to the given precision (number of significant digits)"
  [precision d]
  (let [factor (Math/pow 10 precision)]
    (/ (Math/round (* d factor)) factor)))