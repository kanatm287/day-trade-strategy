(ns real-time-data-process.core
  (:require
    ;;vendor
    [clj-time.coerce :as c]
    [overtone.at-at :as at]
    [clj-time.core :as t]
    ;;local
    [real-time-data-process.database.pull-from-db :as db-pull]
    [real-time-data-process.handlers :as handlers]
    [real-time-data-process.bars.create :as bars]
    [real-time-data-process.manage :as manage]
    [real-time-data-process.utils :as u]
    [ib-client.core :as ib-client]
    [ib-client.time :as ib-time]
    [log.core :as log]
    [db.core :as db])

  (:gen-class))

(def job-pool (at/mk-pool))

(defn start-building-and-analyzing-bars
  [api buy-tickers sell-tickers benchmarks counter]
  (log/info "day trade" "start building bars" nil)
  (doseq [benchmark-data benchmarks]
    (log/info "day trade" "benchmark" (:ticker benchmark-data))
    (bars/create-minute-bars api benchmark-data nil counter))
  (doseq [ticker-data buy-tickers]
    (log/info "day trade" "buy ticker" (:ticker ticker-data))
    (bars/create-minute-bars api ticker-data "BUY" counter))
  (doseq [ticker-data sell-tickers]
    (log/info "day trade" "sell ticker " (:ticker ticker-data))
    (bars/create-minute-bars api ticker-data "SELL" counter)))

(defn manage-open-orders
  []
  (let [open-trades
        (db-pull/pull-data-from-day-trade-orders nil "MKT" nil)]
    (when-not (empty? open-trades)
      (doseq [trade open-trades]
        (manage/open-trade-process trade)))))

(defn working-orders
  [trades]
  (filter #(= (:ib_status %) nil) trades))

(defn manage-stop-orders
  []
  (let [stop-trades
        (working-orders
          (db-pull/pull-data-from-day-trade-orders nil "STOP LOSS" nil))]
    (when-not (empty? stop-trades)
      (doseq [trade stop-trades]
        (manage/stop-trade-process trade)))))

(defn manage-limit-orders
  []
  (let [limit-trades
        (working-orders
          (db-pull/pull-data-from-day-trade-orders nil "TAKE PROFIT" nil))]
    (when-not (empty? limit-trades)
      (doseq [trade limit-trades]
        (manage/limit-trade-process trade)))))

(defn init-ib-id
  []
  (let [counter (atom (or (db-pull/get-last-ib-id) 0))]
    counter))

(defn prop
  [s]
  (System/getProperty s))

(defn shut-down
  [api]
  (log/info "day trade" "shutting down..." nil)
  (at/stop-and-reset-pool! job-pool :strategy :kill)
  (ib-client/stop api)
  (System/exit 0))

(defn -main
  [& args]
  (db/init-database!)
  (log/init-db (db/get-ds))
  (let [api (ib-client/start
              (Integer/parseInt (prop "ib.clientId"))
              {:error handlers/log-error}
              (prop "ib.host")
              (Integer/parseInt (prop "ib.port")))
        buy-tickers (db-pull/pull-valid-tickers :day_trade_buy)
        sell-tickers (db-pull/pull-valid-tickers :day_trade_sell)
        benchmarks (db-pull/pull-benchmarks)
        tickers (concat (map :ticker buy-tickers)
                        (map :ticker sell-tickers)
                        (map :ticker benchmarks))
        counter (init-ib-id)
        time-now (c/to-long (t/now))
        time-end (c/to-long (ib-time/ny-time-in-local-tz 16 05 nil nil))
        app-end (- time-end time-now)]
    (bars/prerequisite-bars api tickers)
    (reset! bars/tickers tickers)
    (reset! bars/ticker-count (count tickers))
    (reset! u/technical-params-map
            (u/create-technical-params-map buy-tickers sell-tickers))
    (start-building-and-analyzing-bars
      api buy-tickers sell-tickers benchmarks counter)
    (at/every 30000 #(manage-open-orders) job-pool)
    (at/every 60000 #(manage-stop-orders) job-pool)
    (at/every 60000 #(manage-limit-orders) job-pool)
    (at/after app-end #(shut-down api) job-pool)))
