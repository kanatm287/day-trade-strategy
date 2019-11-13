(ns financial-data-load.utils
  (:require
    ;;vendor
    ;;local
    [ib-client.client :as ib-client]))

(defn create-contract
  [ticker conid]
  (ib-client/create-contract {:symbol ticker
                              :sec-type "STK"
                              :conid conid
                              :exchange "SMART"
                              :currency "USD"}))

(defn get-contract-details-params
  [api ticker]
  (:contract-details
    (:data
      (ib-client/request-contract
        api
        {:symbol ticker
         :sec-type "STK"
         :exchange "SMART"
         :currency "USD"}))))
