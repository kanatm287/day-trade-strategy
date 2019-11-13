(ns real-time-data-process.manage
  (:require
    ;;vendor
    [clj-time.core :as t]
    [clj-time.coerce :as c]
    ;;local
    [real-time-data-process.database.pull-from-db :as db-pull]
    [real-time-data-process.database.push-to-db :as db-push]
    [real-time-data-process.back-test-params :as back-test]
    [real-time-data-process.utils :refer [precision-round]]
    [real-time-data-process.execution :as exec]
    [ib-client.time :as client-time]
    [log.core :as log]))

(defn last-price
  [ticker]
  (:close (first (db-pull/pull-n-minute-bars ticker 1 "1 min" "close" "desc"))))

(defn fast-close
  [last-price fill-price action ticker signal-id]
  (let [trail-percent (/ (back-test/trail-percent
                           ticker signal-id (if (= action "BUY") "SELL" "BUY"))
                         100)
        trail-percent-half (+ 1 (/ trail-percent 2))]
    (when client-time/trading-hours?
      (if (= action "SELL")
        (> (/ fill-price last-price) trail-percent-half)
        (> (/ last-price fill-price) trail-percent-half)))))

(defn get-fill-data
  [{:keys [ticker signal_id oca_group]}]
  (first
    (filter (fn [x] (and (= (:oca_group x) oca_group)
                         (= (:order_type x) "MKT")))
            (db-pull/pull-data-from-day-trade-orders ticker nil signal_id))))

(defn stop-trade-process
  [trade]
  (let [current-time (t/now)
        trade-time (c/from-sql-time (:date trade))
        last-close (last-price (:ticker trade))
        fill-price (:fill_price (get-fill-data trade))
        ticker (:ticker trade)
        action (:action trade)
        signal-id (:signal_id trade)
        trail-time (back-test/trail-time ticker signal-id)
        min-trail-percent
          (back-test/minimum-trail-percent
            ticker signal-id (if (= action "BUY") "SELL" "BUY"))
        interval (t/in-minutes (t/interval trade-time current-time))]
    (log/manage-order "day trade" "check stop price"
                      (str "ticker " ticker
                           ", action " action
                           ", last price " last-close
                           ", fill price " fill-price
                           ", interval " interval))
    (if (client-time/trading-hours?)
      (if fill-price
        (if (fast-close last-close fill-price action ticker signal-id)
          (do
            (log/manage-order "day trade" "modify order"
                              (str "emergency change trailing percent for "
                                   ticker " to "
                                   0.1))
            (exec/modify-order
              (:ib_id trade)
              0.1))
          (when (and (> interval trail-time)
                     (> (:trailing_percent trade) min-trail-percent))
            (log/manage-order "day trade" "modify order"
                              (str "change trailing percent for " ticker
                                   " from " (:trailing_percent trade)
                                   " to " (* 0.5 (:trailing_percent trade))))
            (exec/modify-order
              (:ib_id trade)
              (* 0.5 (:trailing_percent trade)))))
        (log/manage-order "day trade" "modify order"
                          (str "no fill price for ticker " ticker)))
      (do
        (log/manage-order "day trade" "modify order"
                          "end of trading session create closing orders")
        (exec/modify-order (:ib_id trade) 0.1)))))

(defn machine-learning-condition
  [ticker action]
  (let [column-name (if (= action "BUY") "low_stop" "high_stop")]
    ((keyword column-name)
     (first (db-pull/pull-n-minute-bars
              ticker 1 "1 min" column-name "desc")))))

(defn limit-close-conditions
  [ticker action last-close trade condition]
  (log/manage-order "day trade" "modify order"
                    (str "move limit price close to market price, "
                         "cause limit " condition " condition met for ticker "
                         ticker
                         ", limit price "
                         (precision-round
                                 2
                                 (if (= action "SELL")
                                   (* 1.001 last-close)
                                   (* 0.999 last-close)))))
  (exec/modify-order
    (:ib_id trade)
    (precision-round
      2
      (if (= action "SELL") (* 1.001 last-close) (* 0.999 last-close)))))

(defn limit-trade-process
  [trade]
  (let [action (:action trade)
        ticker (:ticker trade)
        last-close (last-price ticker)
        machine-learning-condition (machine-learning-condition ticker action)
        limit-price (:trailing_percent trade)
        fill-data (get-fill-data trade)
        fill-price (:fill_price fill-data)
        limit-time (back-test/limit-time
                     ticker (:signal_id trade))
        interval (t/in-minutes
                   (t/interval
                     (c/from-sql-time (:date fill-data))
                     (t/now)))]
    (log/manage-order "day trade" "check limit price"
                      (str "ticker "ticker
                           ", action " action
                           ", limit price " limit-price
                           ", interval " interval))
    (cond
      (and (if (= action "SELL")
             (> last-close fill-price)
             (< last-close fill-price))
           (> interval limit-time))
      (limit-close-conditions ticker action last-close trade "time")

      fill-price
      (let [limit-percent
            (back-test/limit-percent
              ticker (:signal_id trade) (if (= action "BUY") "SELL" "BUY"))
            correct-limit-price (precision-round
                                  2
                                  (if (= action "SELL")
                                    (* fill-price limit-percent)
                                    (* fill-price (- 1 (- limit-percent 1)))))]
        (when-not (= limit-price correct-limit-price)
          (log/manage-order "day trade" "modify order"
                            (str "ticker "ticker
                                 " move limit price from "
                                 (:trailing_percent trade) " to " correct-limit-price))
          (exec/modify-order (:ib_id trade) correct-limit-price)))

      machine-learning-condition
      (limit-close-conditions
        ticker action last-close trade
        (if (= action "SELL") "high_stop" "low_stop"))

      :else
      (log/manage-order "day trade" "modify order"
                        (str "no limit price change conditions met for ticker "
                            ticker)))))

(defn open-trade-process
  [trade]
  (let [trade-time (c/to-long (c/from-sql-time (:date trade)))
        time-now (c/to-long (t/minus (t/now) (t/seconds 60)))]
    (when (< trade-time time-now)
      (db-push/write-expired-dt-order-to-db (:ib_id trade)))))
