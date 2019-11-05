(ns historical-data-load.trade-params
  (:require
    ;; Vendor
    ;; Local
    [historical-data-load.database.pull-from-db :as db-pull]
    [historical-data-load.database.push-to-db :as db-push]
    [historical-data-load.database.deletes :as db-del]))

(defn update-hlc-in-trade-params
  [tickers]
  (doall
    (pmap
      (fn [ticker]
        (let [high (db-pull/pull-n-bars ticker 75 "daily" "high" "desc")
              low (db-pull/pull-n-bars ticker 75 "daily" "low" "desc")]
          (if (and (not (empty? high))
                   (not (empty? low)))
            (let [high-max (apply max (map :high high))
                  low-min (apply min (map :low low))
                  three-quarter-average (- high-max (/ (- high-max low-min) 2))]
              (db-push/update-trade-params
                ticker
                {:three-quarter-average_price three-quarter-average
                 :last_price
                 (:close
                   (first (db-pull/pull-n-bars ticker 1 "daily" "close" "desc")))}))
            (db-del/remove-from-trade-params ticker))))
      tickers)))

(defn update-day-trade
  []
  (db-push/update-day-trade-buy)
  (db-push/update-day-trade-sell))

(defn core
  []
  (let [tickers (db-pull/pull-all-tickers)]
    (when-not (empty? tickers)
      (update-hlc-in-trade-params tickers)
      (update-day-trade))))
