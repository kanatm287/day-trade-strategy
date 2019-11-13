(ns real-time-data-process.execution
  (:require
    ;; Vendor
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;; Local
    [real-time-data-process.database.push-to-db :as db-push]
    [real-time-data-process.back-test-params :as back-test]
    [real-time-data-process.utils :refer [precision-round]]))

(defn execute-open-order
  [ticker action signal-id oca-group last-close counter conid]
  (db-push/write-dt-order-to-db
   {:date (c/to-timestamp (t/now))
    :ticker ticker
    :sec-type "STK"
    :order-type "MKT"
    :trailing-percent nil
    :tif "DAY"
    :id (swap! counter inc)
    :action action
    :signal-id signal-id
    :oca-group oca-group
    :last-close last-close
    :conid conid}))

(defn execute-stop-loss-order
  [ticker action signal-id oca-group last-close counter conid]
  (let [trail-percent (back-test/trail-percent ticker signal-id action)]
    (db-push/write-dt-order-to-db
     {:date (c/to-timestamp (t/now))
      :ticker ticker
      :sec-type "STK"
      :order-type "STOP LOSS"
      :trailing-percent trail-percent
      :tif "DAY"
      :id (swap! counter inc)
      :action (if (= action "BUY") "SELL" "BUY")
      :signal-id signal-id
      :oca-group oca-group
      :last-close last-close
      :conid conid})))

(defn execute-take-profit-order
  [ticker action signal-id oca-group last-close counter conid]
  (let [multiplier (back-test/limit-percent ticker signal-id action)
        limit-price (* last-close multiplier)]
    (db-push/write-dt-order-to-db
     {:date (c/to-timestamp (t/now))
      :ticker ticker
      :sec-type "STK"
      :order-type "TAKE PROFIT"
      :trailing-percent (precision-round 2 limit-price)
      :tif "DAY"
      :id (swap! counter inc)
      :action (if (= action "BUY") "SELL" "BUY")
      :signal-id signal-id
      :oca-group oca-group
      :last-close last-close
      :conid conid})))

(defn modify-order
  [req-id trailing]
  (db-push/write-processed-dt-order-to-db
   req-id nil nil (c/to-timestamp (t/now)) trailing))
