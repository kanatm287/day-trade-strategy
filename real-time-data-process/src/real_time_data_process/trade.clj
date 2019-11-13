(ns real-time-data-process.trade
  (:require
    ;; Vendor
    ;;local
    [real-time-data-process.database.pull-from-db :as db-pull]
    [real-time-data-process.back-test-params :as back-test]
    [real-time-data-process.conditions :as cond]
    [real-time-data-process.execution :as exec]
    [ib-client.state :as client-state]
    [ib-client.time :as client-time]
    [log.core :as log]))

(defn get-fill-price
  [position order-type]
  (:fill_price (first (filter #(= (:order_type %) order-type) position))))

(defn process-positions
  [max-trades-per-day positions order-type]
  (if (= max-trades-per-day 1)
    true
    (if (= (count positions) 3)
      false
      (let [filtered-positions
            (remove nil? (filter #(= (:order_type %) order-type) positions))]
        (if (< (count filtered-positions) max-trades-per-day) false true)))))

(defn rest-limit-positions
  [positions limit-trades]
  (process-positions limit-trades positions "TAKE PROFIT"))

(defn rest-stop-positions
  [positions stop-trades]
  (process-positions stop-trades positions "STOP LOSS"))

(defn generate-log-string
  [ticker positions strategy action last-mkt-price field field-price]
  (str  "ticker " ticker
        ", positions " (/ (count positions) 3)
        ", strategy " strategy
        ", action " action
        ", market price " last-mkt-price
        field field-price))

(defn check-positions-for-existence?
  [ticker strategy action]
  (let [positions
          (db-pull/pull-data-from-day-trade-orders ticker "MKT" nil)
        stop_trades_per_day
          (back-test/stop-trades-per-day ticker strategy)
        limit_trades_per_day
          (back-test/limit-trades-per-day ticker strategy)]
    (when-not (empty? positions)
      (if (<= (count positions) (+ stop_trades_per_day limit_trades_per_day))
        (let [last-position (take 3 positions)
              last-mkt-price (get-fill-price last-position "MKT")
              last-lmt-price (get-fill-price last-position "TAKE PROFIT")
              last-stp-price (get-fill-price last-position "STOP LOSS")]
          (log/execute-order "day trade"
                             (str strategy)
                             (str "checking positions for " ticker))
          (cond
            last-lmt-price
            (do
              (log/execute-order
                "day trade"
                "close position with limit order"
                (generate-log-string
                  ticker positions strategy action
                  last-mkt-price ", limit price " last-lmt-price))
              (rest-limit-positions positions limit_trades_per_day))
            last-stp-price
            (do
              (if (= action "BUY")
                (if (> last-stp-price last-mkt-price)
                    (log/execute-order
                      "day trade"
                      "position closed with stop order in profit"
                      (generate-log-string
                        ticker positions strategy action
                        last-mkt-price
                        ", stop price " last-stp-price))
                    (log/execute-order
                      "day trade"
                      "position closed with stop order in loss"
                      (generate-log-string
                        ticker positions strategy action
                        last-mkt-price
                        ", stop price " last-stp-price)))
                (if (< last-stp-price last-mkt-price)
                    (log/execute-order
                      "day trade"
                      "position closed with stop order in profit"
                      (generate-log-string
                        ticker positions strategy action
                        last-mkt-price
                        ", stop price " last-stp-price))
                    (log/execute-order
                      "day trade"
                      "position closed with stop order in loss"
                      (generate-log-string
                        ticker positions strategy action
                        last-mkt-price
                        ", stop price " last-stp-price))))
              (rest-stop-positions positions stop_trades_per_day))
            :else
            true))
        true))))

(defn execute-trading
  [api ticker action signal-id last-close counter conid]
  (when (client-time/trading-hours?)
    (let [oca-group (str ticker "_" (client-state/new-req-id api))]
      (exec/execute-open-order
        ticker action signal-id oca-group last-close counter conid)
      (exec/execute-stop-loss-order
        ticker action signal-id oca-group last-close counter conid)
      (exec/execute-take-profit-order
        ticker action signal-id oca-group last-close counter conid))))

(defn trend
  [api ticker action bar counter conid]
  (when (back-test/check-back-test-performance? ticker "trend" action)
    (when-not (boolean (check-positions-for-existence? ticker "trend" action))
      (when (and
              (cond/base-breakaway-condition-is-met? bar action)
              (if (back-test/use-ml? ticker "trend")
                (cond/trend-ml-conditions? bar action)
                true)
              (if (back-test/use-benchmark? ticker "trend")
                (cond/benchmark-conditions? bar)
                true))
        (log/execute-order "day trade" "executing trend"
                           (str action " for ticker " ticker))
        (execute-trading api ticker action
                         "trend" (:close bar) counter conid)))))

(defn correction
  [api ticker action bar counter conid]
  (when (back-test/check-back-test-performance? ticker "correction" action)
    (when-not (boolean (check-positions-for-existence?
                         ticker "correction" action))
      (when (and
              (cond/base-breakaway-condition-is-met? bar action)
              (if (back-test/use-ml? ticker "correction")
                (cond/correction-ml-conditions? bar action)
                true)
              (if (back-test/use-benchmark? ticker "correction")
                (cond/benchmark-conditions? bar)
                true))
        (log/execute-order "day trade" "executing correction"
                           (str action " for ticker " ticker))
        (execute-trading api ticker action
                         "correction" (:close bar) counter conid)))))

(defn start-trading
  [api ticker action counter conid]
  (let [bar (first (db-pull/pull-n-minute-bars ticker 1 "1 min" "*" "desc"))]
    (cond
      (= action "BUY")
      (if (> (:close bar) (:prev_day_close bar))
        (trend api ticker action bar counter conid)
        (correction api ticker action bar counter conid))
      (= action "SELL")
      (if (< (:close bar) (:prev_day_close bar))
        (trend api ticker action bar counter conid)
        (correction api ticker action bar counter conid)))))
