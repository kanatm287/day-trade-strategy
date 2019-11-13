(ns thin-client.http-request
  (:require
   ;;vendor
   [org.httpkit.client :as http]
   [clojure.data.json :as json]))

(defonce STRATEGY_SERVER_URL
         (System/getProperty "strategy.server" "http://localhost:7777"))

(defn json-request
  ([data]
   (json-request data 5000))
  ([data timeout]
   {:body (json/write-str data)
    :headers {"Content-Type" "application/json"}
    :timeout timeout}))

(defn perform-request
  ([http-fn path]
   (perform-request http-fn path nil))
  ([http-fn path data]
   (perform-request http-fn path data false))
  ([http-fn path data json?]
   (let [url (str STRATEGY_SERVER_URL path)
         {:keys [status body error]} (if data
                                       @(http-fn url (json-request data))
                                       @(http-fn url))]
     (if (= 200 status)
       (if json?
         (-> body (json/read-str :key-fn keyword))
         body)
       (println (str "Server error: " status error ", for url: " url))))))

(defn open-session
  []
  (perform-request http/get "/open-session" nil true))

(defn error
  [message]
  (perform-request http/post "/error" message))

(defn insert-execution
  [execution]
  (perform-request http/post "/execution" execution))

(defn update-commission
  [commission]
  (perform-request http/put "/commission" commission))

(defn update-ib-id
  [id]
  (perform-request http/put "/ib-id" id))

(defn update-order-statuses
  [statuses]
  (perform-request http/put "/statuses" {:data statuses}))

(defn update-encrypted-order-statuses
  [encrypted-statuses]
  (perform-request http/put "/encrypted-statuses" {:data encrypted-statuses}))

(defn request-day-trade-instructions
  [ib-account-id balance]
  (perform-request http/post "/instructions"
                   {:ib-account-id ib-account-id
                    :strategy-id "day-trade"
                    :balance balance}
                   true))

(defn request-short-term-instructions
  [ib-account-id balance]
  (perform-request http/post "/instructions"
                   {:ib-account-id ib-account-id
                    :strategy-id "short-term"
                    :balance balance}
                   true))

(defn request-bond-instructions
  [ib-account-id balance positions]
  (perform-request http/post "/instructions"
                   {:ib-account-id ib-account-id
                    :strategy-id "bond"
                    :balance balance
                    :positions positions}
                   true))

(defn request-order-instruction
  [ib-account-id ib-order-id]
  (let [url (str "/order/" ib-order-id "?ib-account-id=" ib-account-id)]
    (perform-request http/get url nil true)))

(defn check-position-quantity
  [ib-account-id positions]
  (perform-request http/post "/spot-positions"
                   {:ib-account-id ib-account-id
                    :positions positions}
                   true))