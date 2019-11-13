(ns real-time-data-process.conditions
  (:require
   ;; Vendor
   ;; Local
   [real-time-data-process.utils :as u]))

; ---------------============ Trend Follow Conditions ============---------------

(defn base-breakaway-condition-is-met?
  [bar action]
  (if (= action "BUY")
    (:buy_signal bar)
    (:sell_signal bar)))

; -------------=========== Machine Learning Conditions ===========-------------

(defn trend-ml-conditions?
  [bar action]
  (let [ticker (:ticker bar)
        high-delta-permission
        (:high_delta (:trend ((keyword ticker) @u/technical-params-map)))
        high-stop-permission
        (:high_stop (:trend ((keyword ticker) @u/technical-params-map)))
        low-delta-permission
        (:low_delta (:trend ((keyword ticker) @u/technical-params-map)))
        low-stop-permission
        (:low_stop (:trend ((keyword ticker) @u/technical-params-map)))]
    (if (= action "BUY")
      (let [high-delta (when high-delta-permission (:high_delta bar))
            high-stop (when high-stop-permission (not (:high_stop bar)))]
        (or high-delta high-stop))
      (let [low-delta (when low-delta-permission (:low_delta bar))
            low-stop (when low-stop-permission (not (:low_stop bar)))]
        (or low-delta low-stop)))))

(defn correction-ml-conditions?
  [bar action]
  (let [ticker (:ticker bar)
        high-delta-permission
        (:high_delta (:correction ((keyword ticker) @u/technical-params-map)))
        high-stop-permission
        (:high_stop (:correction ((keyword ticker) @u/technical-params-map)))
        low-delta-permission
        (:low_delta (:correction ((keyword ticker) @u/technical-params-map)))
        low-stop-permission
        (:low_stop (:correction ((keyword ticker) @u/technical-params-map)))]
    (if (= action "BUY")
      (let [high-delta (when high-delta-permission (:high_delta bar))
            low-stop (when low-stop-permission (:low_stop bar))]
        (or high-delta low-stop))
      (let [low-delta (when low-delta-permission (:low_delta bar))
            high-stop (when high-stop-permission (:high_stop bar))]
        (or low-delta high-stop)))))

; ---------------============ Benchmark Conditions ============---------------

(defn benchmark-conditions?
  [bar]
  (when-not (nil? (:benchmark_trend bar))
    (:benchmark_trend bar)))
