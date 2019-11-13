(ns real-time-data-process.bars.create
  (:require
    ;; Vendor
    [clojure.core.async :refer [go-loop <!]]
    [clj-time.coerce :as c]
    [clj-time.format :as f]
    [clj-time.core :as t]
    ;;local
    [ib-client.state :as client-state]
    [ib-client.time :as client-time]
    [ib-client.wrapper :as wrapper]
    [ib-client.utils :as ib-utils]
    [ib-client.client :as client]
    [real-time-data-process.database.pull-from-db :as db-pull]
    [real-time-data-process.database.push-to-db :as db-push]
    [real-time-data-process.database.functions :as db-func]
    [real-time-data-process.database.deletes :as db-del]
    [real-time-data-process.scripts :as scripts]
    [real-time-data-process.bars.utils :as u]
    [real-time-data-process.utils :as common-uitls]
    [real-time-data-process.trade :as trade]
    [log.core :as log]))

(defn day-open
  [api ticker]
  (when-let [hist-data (ib-utils/request-ticker-historical-data
                         api
                         (u/create-contract
                           ticker
                           (:conid (db-pull/pull-fundamental-params ticker)))
                         "5 mins"
                         "1 D"
                         nil)]
    (let [prev_day_close
          (:prev_day_close
            (db-pull/pull-previous-day-close-bar ticker))
          bars (map :data hist-data)
          open-bar (u/day-open-bar bars)]
      (db-del/delete-today-created-bars
        ticker
        (c/to-timestamp
          (f/parse
            (f/formatter "yyyyMMdd  HH:mm:ss")
            (:date open-bar))))
      (assoc-in
        @common-uitls/basic-bar-params-map
        [(keyword ticker) :day_open]
        (:open open-bar))
      (doseq [bar bars]
        (when bar
          (log/info
            "day trade"
            "historical data"
            (str "writing day open"
                 " historical data to db for ticker " ticker))
          (db-func/historical-data-writing-function ticker bar "5 mins")
          (db-push/write-value-to-db
            (:open open-bar) "day_open" ticker
            (c/to-timestamp
              (f/parse
                (f/formatter "yyyyMMdd  HH:mm:ss")
                (:date bar)))
            "5 mins")
          (db-push/write-value-to-db
            prev_day_close "prev_day_close" ticker
            (c/to-timestamp
              (f/parse
                (f/formatter "yyyyMMdd  HH:mm:ss")
                (:date bar)))
            "5 mins"))))))

(defn previous-day-close
  [api ticker]
  (when-let [hist-data (ib-utils/request-ticker-historical-data
                         api
                         (u/create-contract
                           ticker
                           (:conid (db-pull/pull-fundamental-params ticker)))
                         "1 min"
                         "18000 s"
                         nil)]
    (let [bar (u/previous-day-close-bar (map :data hist-data))]
       (log/info
         "day trade"
         "historical data"
         (str "writing previous day close"
              " historical data to db for ticker " ticker))
       (assoc-in
         @common-uitls/basic-bar-params-map
         [(keyword ticker) :prev_day_close]
         (:close bar))
       (db-del/clean-historical-minute-data ticker)
       (db-func/historical-data-writing-function ticker bar "1 min")
       (db-push/write-value-to-db
         (:close bar)
         "prev_day_close"
         ticker
         (c/to-timestamp
           (f/parse
             (f/formatter "yyyyMMdd  HH:mm:ss")
             (:date bar)))
         "1 min"))))

(defn prerequisite-bars
  [api tickers]
  (doseq [ticker tickers]
    (previous-day-close api ticker)
    (when (client-time/session-open?)
      (day-open api ticker))))

(def tickers (atom nil))
(def ticker-count (atom nil))

(defn watcher
  [_ a _ tc]
  (when (= tc 0)
    (scripts/prepare-real-time-counter-data)
    (scripts/prepare-real-time-benchmark-data)
    (let [benchmarks (map :ticker (db-pull/pull-benchmarks))]
      (doall
        (pmap
          (fn [ticker]
            (when-not (some #(= ticker %) benchmarks)
              (log/info "day trade" "preparing real-time 5 minute data for" ticker)
              (scripts/prepare-real-time-data ticker "5 mins")
              (scripts/machine-learning-functions ticker)))
          @tickers)))
    (remove-watch a :my-watch)
    (reset! a (count @tickers))
    (add-watch a :my-watch watcher)))

(add-watch ticker-count :my-watch watcher)

(defn create-five-minute-bars
  [ticker industry action]
  (let [xs (reverse (db-pull/pull-n-minute-bars ticker 5 "1 mins" "*" "desc"))]
    (if (u/fifth-minute? (:date (first xs)))
      (let [bar (reduce (fn [current next]
                          (let [{:keys [date open high low close volume count]}
                                current]
                            {:date date
                             :open open
                             :high (if (> (:high next) high)
                                     (:high next)
                                     high)
                             :low (if (< (:low next) low)
                                    (:low next)
                                    low)
                             :close (:close next)
                             :volume (+ volume (:volume next))
                             :count (+ count (:count next))
                             :has_gaps nil}))
                        (first xs)
                        (rest xs))]
        (log/info "day trade" "writing 5 minute bar to db for"
                  (str ticker " " (:date bar)))
        (db-func/historical-data-writing-function ticker bar "5 mins")
        (db-push/add-industry ticker industry "5 mins")
        (if (client-time/session-start? (u/stamp-to-datetime (:date bar)))
          (let [prev_day_close (:prev_day_close
                                 (db-pull/pull-previous-day-close-bar ticker))]
            (log/info "day trade" "writing open price to db for"
                      (str ticker " " (:date bar) " " (:open bar)))
            (db-push/write-value-to-db
              (:open bar) "day_open" ticker nil "5 mins")
            (db-push/write-value-to-db
              prev_day_close "prev_day_close" ticker nil "5 mins")
            (assoc-in
              @common-uitls/basic-bar-params-map
              [(keyword ticker) :prev_day_close]
              prev_day_close)
            (assoc-in
              @common-uitls/basic-bar-params-map
              [(keyword ticker) :day_open]
              (:open bar))))
        (do
          (db-push/write-value-to-db
            (:day_open (:ticker @common-uitls/basic-bar-params-map))
            "day_open"
            ticker
            nil
            "5 mins")
          (db-push/write-value-to-db
            (:prev_day_close (:ticker @common-uitls/basic-bar-params-map))
            "prev_day_close"
            ticker
            nil
            "5 mins"))
        (swap! ticker-count dec))
      (do
        (if (= action "BUY")
          (u/pull-and-write-minute-bar ticker "high_delta" "1 min")
          (u/pull-and-write-minute-bar ticker "low_delta" "1 min"))
        (u/pull-and-write-minute-bar ticker "high_stop" "1 min")
        (u/pull-and-write-minute-bar ticker "low_stop" "1 min")
        (u/pull-and-write-minute-bar ticker "benchmark_trend" "1 min")))))

(defn create-minute-bars
  [api {:keys [ticker conid industry]} action counter]
  (let [req-id (client-state/new-req-id api)
        rlt-bar-chan (client/request-realtime-bar
                      api
                      (u/create-contract ticker conid)
                      req-id)
        benchmarks (map :ticker (db-pull/pull-benchmarks))]
    (go-loop [rlt-resp (<! rlt-bar-chan)
              rlt-bar nil]
      (if (and (wrapper/success-response? rlt-resp)
               (> (c/to-epoch (t/now)) (client-time/session-start-in-stamp)))
        (let [new-rlt-bar (:data rlt-resp)
              rlt-time (:time new-rlt-bar)]
          (cond

            (not (client-time/session-open?))
            (let [midnight (c/to-timestamp (t/today-at-midnight))]
              (log/info "day trade" "session end"
                        (str "cancel real time data, "
                             "delete today created bars in database "
                             "for ticker " ticker))
              (client/cancel-realtime-bar api req-id)
              (db-del/delete-today-created-bars ticker midnight))

            (u/minute-beginning? rlt-time)
            (recur (<! rlt-bar-chan) (u/first-rlt-bar new-rlt-bar))

            (u/minute-ending? rlt-time)
            (do
              (log/info "day trade" "writing to db minute bar data for " ticker)
              (db-func/historical-data-writing-function
                ticker
                (u/modify-rlt-bar (if rlt-bar rlt-bar new-rlt-bar) new-rlt-bar)
                "1 min")
              (db-push/add-industry ticker industry "1 min")
              (log/info
                "day trade"
                "calculating and updating to db minute data for " ticker)
              (create-five-minute-bars ticker industry action)
              (when-not (some #(= ticker %) benchmarks)
                (scripts/prepare-real-time-data ticker "1 min")
                (db-push/write-value-to-db
                  (:prev_day_close (:ticker @common-uitls/basic-bar-params-map))
                  "prev_day_close"
                  ticker
                  nil
                  "1 min")
                (trade/start-trading api ticker action counter conid))
              (recur (<! rlt-bar-chan) nil))

            :else
            (recur (<! rlt-bar-chan)
                   (u/modify-rlt-bar (if rlt-bar rlt-bar new-rlt-bar)
                    new-rlt-bar))))
        (recur (<! rlt-bar-chan) nil)))))
