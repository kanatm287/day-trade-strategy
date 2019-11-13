(ns real-time-data-process.database.push-to-db
  (:require
   ;; vendor
   [clojure.java.jdbc :as j]
   [honeysql.core :as sql]
   ;; local
   [db.core :as db]))

; ==============================================================================
; -------------------=========== Non Collective 2 ===========-------------------
; ==============================================================================
; ----------------============== Day Trading Data ==============----------------

(defn write-value-to-db
  [value column-name ticker date time-frame]
  (j/execute! (db/get-ds)
    (sql/format {:update (cond
                           (= time-frame "5 mins")
                           :historical_five_minute_data

                           (= time-frame "1 min")
                           :historical_minute_data)
                 :set {(keyword column-name) value}
                 :where [:and
                         [:= :ticker ticker]
                         [:= :date
                          (if date
                            date
                            {:select [:date]
                             :from [(if (= time-frame "5 mins")
                                      :historical_five_minute_data
                                      :historical_minute_data)]
                             :where [:= :ticker ticker]
                             :order-by [[:date :desc]]
                             :limit 1})]]})))

(defn write-dt-order-to-db
  [{:keys
    [date ticker sec-type order-type trailing-percent
     tif id action last-close oca-group signal-id conid]}]
  (j/execute! (db/get-ds)
    (sql/format   {:insert-into :day_trade_orders
                   :values [{:date date
                             :ticker ticker
                             :sec_type sec-type
                             :order_type order-type
                             :trailing_percent trailing-percent
                             :tif tif
                             :ib_id id
                             :action action
                             :last_close last-close
                             :oca_group oca-group
                             :signal_id signal-id
                             :conid conid}]})))

(defn write-processed-dt-order-to-db
  [ib-id status traded-price date trailing]
  (let [set (if date
              {:date date
               :trailing_percent trailing}
              {:ib_status (name status)
               :traded_price traded-price})]
    (j/execute! (db/get-ds)
      (sql/format {:update :day_trade_orders
                   :set set
                   :where [:= :ib_id ib-id]}))))

(defn write-expired-dt-order-to-db
  [ib-id]
  (j/execute! (db/get-ds)
    (sql/format {:update :day_trade_orders
                 :set {:ib_status "expired"}
                 :where [:= :ib_id ib-id]})))

(defn add-industry
  [ticker industry time-frame]
  (let [table (if (= time-frame "1 min")
                :historical_minute_data
                :historical_five_minute_data)]
    (j/execute! (db/get-ds)
      (sql/format {:update table
                   :set {:industry industry}
                   :where [:and
                           [:= :ticker ticker]
                           [:= :date {:select [:date]
                                      :from [table]
                                      :order-by [[:date :desc]]
                                      :limit 1}]]}))))

