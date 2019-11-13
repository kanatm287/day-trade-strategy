(ns real-time-data-process.back-test-params
  (:require
    ;; vendor
    ;; local
    [real-time-data-process.utils :as u]))

(defn get-technical-params
  [ticker strategy]
  ((keyword strategy) ((keyword ticker) @u/technical-params-map)))

(defn use-benchmark?
  [ticker strategy]
  (let [technical-params (get-technical-params ticker strategy)
        benchmark-positive (:benchmark_positive technical-params)
        benchmark-negative (:benchmark_negative technical-params)]
    (and benchmark-positive benchmark-negative)))

(defn use-ml?
  [ticker strategy]
  (let [technical-params (get-technical-params ticker strategy)
        high-delta (:high_delta technical-params)
        low-delta (:low_delta technical-params)
        high-stop (:high_stop technical-params)
        low-stop (:low_stop technical-params)]
    (or high-delta low-delta high-stop low-stop)))

(defn trail-time
  [ticker strategy]
  (:trail_time (get-technical-params ticker strategy)))

(defn stop-trades-per-day
  [ticker strategy]
  (let [mtpd (:stop_trades (get-technical-params ticker strategy))]
    (if mtpd (* mtpd 3) 6)))

(defn limit-trades-per-day
  [ticker strategy]
  (let [mtpd (:limit_trades (get-technical-params ticker strategy))]
    (if mtpd (* mtpd 3) 3)))

(defn minimum-trail-percent
  [ticker strategy action]
  (when-let [mtp (:min_trail_percent (get-technical-params ticker strategy))]
    (if (= action "BUY") (* (- 1 mtp) 100) (* (- mtp 1) 100))))

(defn trail-percent
  [ticker strategy action]
  (let [tp (:trail_percent (get-technical-params ticker strategy))]
    (if tp
      (u/precision-round
        2
        (if (= action "BUY")
          (* (- 1 tp) 100)
          (* (- tp 1) 100)))
      2)))

(defn limit-percent
  [ticker strategy action]
  (let [lp (:limit_price (get-technical-params ticker strategy))]
    (if lp lp (if (= action "BUY") 1.005 0.995))))

(defn limit-time
  [ticker strategy]
  (if-let [lp (:limit_time (get-technical-params ticker strategy))]
    lp
    60))

(defn check-back-test-performance?
  [ticker strategy action]
  (u/get-back-test-param ticker strategy action "performance"))
