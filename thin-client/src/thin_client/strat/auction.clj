(ns thin-client.strat.auction
  (:require
    ;; Vendor
    [clojure.core.async :refer [go-loop <!]]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    [clojure.string :as string]
    ;; Local
    [ib-client.client :as ib-client]
    [ib-client.state :as client-state]
    [ib-client.wrapper :as ew]
    [thin-client.http-request :as reqs]
    [thin-client.exec-utils :as exec]
    [thin-client.execution :as trade]
    [thin-client.utils :as u]))

(defn try-and-check-if-executes?
  [contract ins ask-price bid-price trade-price]
  (let [action (:buy_or_sell ins)
        mid-price (/ (+ ask-price trade-price) 2)
        symbol (:symbol ins)]
    (if (= symbol "HYG")
      (let [limit-price
            (if (= action "SELL")
              (if (< trade-price ask-price) trade-price ask-price)
              (if (> trade-price bid-price) trade-price bid-price))]
        (trade/execute-limit-option-instruction
          contract (assoc ins :limit_price limit-price)))
      (when (if (= action "BUY")
              (<= trade-price mid-price)
              (>= trade-price mid-price))
        (trade/execute-limit-option-instruction
          contract (assoc ins :limit_price trade-price))
        (Thread/sleep 10000)))))

(defn search-best-price-and-execute
  [contract instruction]
  (ib-client/request-market-data-type @exec/api-atom 3)
  (let [req-id (client-state/new-req-id @exec/api-atom)
        mkt-resp-chan
        (ib-client/request-market-data @exec/api-atom contract "221")]
    (when
      (reqs/update-ib-id
        {:instruction-id (:instruction_id instruction)
         :ib-id req-id})
      (let [ins (assoc instruction :ib_id req-id)]
        (go-loop [mkt-resp (<! mkt-resp-chan)
                  old-ask nil
                  old-bid nil
                  multiplier 1.0
                  current-time (c/to-long (t/now))]
                 (if (and (ew/success-response? mkt-resp)
                          (= (:status mkt-resp) :tick-price))
                   (let [{:keys [field price]} (:data mkt-resp)
                         ask (if (= field 2) price old-ask)
                         bid (if (= field 1) price old-bid)]
                     (if (and
                           (or (not= old-ask ask)
                               (not= old-bid bid))
                           ask
                           bid)
                       (if (exec/filled? req-id)
                         (ib-client/cancel-market-data
                           @exec/api-atom (:req-id (:data mkt-resp)))
                         (let [limit-price
                               (if (:limit_price ins)
                                 (:limit_price ins)
                                 (if (= (:buy_or_sell ins) "BUY")
                                   bid ask))
                               trade-price
                               (if (= (:buy_or_sell ins) "BUY")
                                 (if (< limit-price bid) limit-price bid)
                                 (if (> limit-price ask) limit-price ask))]
                           (try-and-check-if-executes?
                             contract ins ask bid (* trade-price multiplier))
                           (recur (<! mkt-resp-chan)
                                  ask
                                  bid
                                  (u/get-multiplier
                                    current-time multiplier ins trade-price)
                                  (u/get-time current-time))))
                       (recur (<! mkt-resp-chan)
                              ask
                              bid
                              (u/get-multiplier
                                current-time multiplier ins (:limit_price ins))
                              (u/get-time current-time))))
                   (recur (<! mkt-resp-chan)
                          old-ask
                          old-bid
                          (u/get-multiplier
                            current-time multiplier ins (:limit_price ins))
                          (u/get-time current-time))))))))

(defn limit-instruction
  [ins]
  (let [contract (u/request-contract @exec/api-atom ins)]
    (search-best-price-and-execute contract ins)))

(defn market-instruction
  [ins]
  (let [contract (u/request-contract @exec/api-atom ins)
        conids (if (:conids ins)
                 (:conids ins)
                 (.conid contract))]
    (search-best-price-and-execute
      contract (assoc ins :conids conids))))