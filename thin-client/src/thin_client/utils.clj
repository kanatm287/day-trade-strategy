(ns thin-client.utils
  (:require
    ;;vendor
    [clojure.string :as string]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;;local
    [ib-client.wrapper :as ew]
    [ib-client.client :as ib-client]
    [thin-client.exec-utils :as order-atom]))

(defn positions-map
  [positions]
  (reduce (fn [val coll]
            (conj
              val
              {:contract
               {:symbol (.symbol (:contract coll))
                :sec-type (.getApiString (.secType (:contract coll)))
                :ltd-or-cm (.lastTradeDateOrContractMonth (:contract coll))
                :strike (.strike (:contract coll))
                :right (.getApiString (.right (:contract coll)))
                :multiplier (.multiplier (:contract coll))
                :currency (.currency (:contract coll))}
               :position (:pos coll)
               :traded-price (:avgCost coll)}))
          '()
          positions))

(defn get-positions
  [api ib-account-id]
  (positions-map
    (filter
      (fn [position]
        (and (not= (:pos position) 0.0)
             (= (:account position) ib-account-id)))
      (map :data (ib-client/request-positions api)))))

(defn get-stock-positions
  [api ib-account-id]
  (filter
    #(and (not= (:symbol (:contract %)) "HYG")
          (= (:sec-type (:contract %)) "STK"))
    (get-positions api ib-account-id)))

(defn get-account-balance
  [api ib-account-id]
  (let [net-liquidation (filter
                          (fn [x]
                            (and
                              (= "NetLiquidationByCurrency" (:tag x))
                              (= "BASE" (:currency x))
                              (= ib-account-id (:account x))))
                          (map :data (ib-client/request-account-summary api)))]
    (Float/parseFloat (string/replace (:value (first net-liquidation)) "," ""))))

(defn get-ib-account-ids
  [api]
  (let [accounts (ib-client/request-accounts api)
        ib-account-ids (if (.contains accounts ",")
                         (string/split accounts #",")
                         [accounts])]
    ib-account-ids))

(defn position-exists?
  [{:keys [account quantity symbol strike put_or_call expire]} positions]
  (some (fn [x]
          (and (= (Math/abs (:pos x)) quantity)
               (= (:account x) account)
               (= (.symbol (:contract x)) symbol)
               (cond
                 (= (.getApiString (.secType (:contract x))) "OPT")
                 (and (= (.symbol (:contract x)) symbol)
                      (= (.strike (:contract x)) strike)
                      (= (.getApiString (.right (:contract x)))
                         put_or_call)
                      (= (.lastTradeDateOrContractMonth (:contract x))
                         expire))
                 (= (.getApiString (.secType (:contract x))) "STK")
                 (= (.symbol (:contract x)) symbol)
                 :else
                 true)))
        positions))

(defn open-order-exists?
  [{:keys [account quantity symbol order_type strike put_or_call expire]}]
  (some (fn [x] (and (or (not= (:status x) :filled)
                         (not= (:status x) :cancelled))
                     (= (:total-quantity (:order x)) quantity)
                     (= (:order-type (:order x)) order_type)
                     (= (:account (:order x)) account)
                     (cond
                       (= (.getApiString (.secType (:contract x))) "OPT")
                       (and (= (.symbol (:contract x)) symbol)
                            (= (.strike (:contract x)) strike)
                            (= (.getApiString (.right (:contract x))) put_or_call)
                            (= (.lastTradeDateOrContractMonth (:contract x)) expire))
                       (= (.getApiString (.secType (:contract x))) "STK")
                       (= (.symbol (:contract x)) symbol)
                       :else
                       true)))
        (order-atom/get-all-orders)))

(defn close-order-id-exists?
  [{:keys [account quantity symbol order_type ib_id]}]
  (some (fn [x]
          (and (or (not= (:status x) :filled)
                   (not= (:status x) :cancelled))
               (= (:order-type (:order x)) (if (= order_type "STP") "TRAIL" "LMT"))
               (= (.symbol (:contract x)) symbol)
               (= (:account (:order x)) account)
               (= (:total-quantity (:order x)) quantity)
               (= (:id x) ib_id)))
        (order-atom/get-all-orders)))

(defn mkt-instructions
  [ins]
  (filter (fn [x] (= (:order_type x) "MKT")) ins))

(defn stp-instructions
  [ins]
  (filter (fn [x] (= (:order_type x) "STP")) ins))

(defn lmt-instructions
  [ins]
  (filter (fn [x] (= (:order_type x) "LMT")) ins))

(defn get-contract-id
  [search-string conids]
  (Integer/parseInt
    (last
      (string/split
        (first
          (filter #(.contains % search-string) conids))
        #"-"))))

(defn check-five-minutes?
  [current-time]
  (> (c/to-long (t/now))
     (+ current-time 300000)))

(defn get-multiplier
  [current-time price-changer ins limit-price]
  (if (check-five-minutes? current-time)
    (if (= (:symbol ins) "HYG")
      (if (= (:buy_or_sell ins) "BUY")
        (if (> limit-price 0) 1.1 0.95)
        0.95)
      (cond
        (and (> limit-price -0.5) (< limit-price 0))
        (if (= (:buy_or_sell ins) "BUY") -1 1.25)

        (and (< limit-price 0.5) (> limit-price 0))
        (if (= (:buy_or_sell ins) "BUY") 1.25 -1)

        (> limit-price 0.5)
        (if (= (:buy_or_sell ins) "BUY") 1.1 0.9)

        (< limit-price -0.5)
        (if (= (:buy_or_sell ins) "BUY") 0.9 1.1)))

    price-changer))

(defn get-time
  [current-time]
  (if (check-five-minutes? current-time)
    (c/to-long (t/now))
    current-time))

(defn create-contract
  [conid action]
  {:conid conid
   :ratio 1
   :action action
   :exchange "SMART"})

(defn- combo-contract
  [api {:keys [symbol conids]}]
  (ib-client/request-combo-contract
    api
    {:symbol symbol
     :sec-type "BAG"
     :currency "USD"
     :exchange "SMART"}
    [(create-contract (:buy-call conids) "BUY")
     (create-contract (:sell-call conids) "SELL")
     (create-contract (:buy-put conids) "SELL")
     (create-contract (:sell-put conids) "BUY")]))

(defn request-contract
  [api ins]
  (if (map? (:conids ins))
    (combo-contract api ins)
    (.contract
      (:contract-details
        (:data
          (ib-client/request-contract
            api
            {:symbol (:symbol ins)
             :sec-type (:security_type ins)
             :strike (:strike ins)
             :ltd-or-cm (:expire ins)
             :right (:put_or_call ins)}))))))

(def hist-param
  {:end-date-time nil
   :duration-str "10 D"
   :bar-size-setting "1 days"
   :what-to-show "TRADES"})

(defn request-last-close
  [api contract-params]
  (let [hist-data
        (ib-client/request-historical-data api contract-params hist-param)]
    (when (ew/success-response? hist-data)
      (-> hist-data last :data :close))))

(defn precision-round
  "Round a double to the given precision (number of significant digits)"
  [precision d]
  (let [factor (Math/pow 10 precision)]
    (/ (Math/round (* d factor)) factor)))