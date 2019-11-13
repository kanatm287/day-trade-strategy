(ns screener.day-trade

  (:require
    ;; vendor
    [clojure.string :as str]
    ;; local
    [screener.jsoup :as jsoup]
    [screener.consts :as C]))

(defonce spy-relative-volume
  (-> C/DAY-TRADE-SPY-URL
      jsoup/get-page
      (.select C/DAY-TRADE-SPY-TABLE)
      (.select C/TABLE)
      .text
      (str/split #" ")
      last
      (str/replace #"%" "")
      Float/parseFloat))

(defn process-finviz-url
  [url]
  (-> url
    jsoup/get-page
    (.select C/CONTENT-ID)
    (.select C/CONTENT-TABLE)
    .text
    (str/split #" ")))

(defn main
  []
  (process-finviz-url
    (str (first C/DAY-TRADE-MAIN)
         (- spy-relative-volume
            (rem spy-relative-volume 0.25))
         (last C/DAY-TRADE-MAIN))))

(defn new-high
  []
  (process-finviz-url
    (str (first C/DAY-TRADE-NEW-HIGH)
         (- spy-relative-volume
            (rem spy-relative-volume 0.25))
         (last C/DAY-TRADE-NEW-HIGH))))

(defn new-low
  []
  (process-finviz-url
    (str (first C/DAY-TRADE-NEW-LOW)
         (- spy-relative-volume
            (rem spy-relative-volume 0.25))
         (last C/DAY-TRADE-NEW-LOW))))

(defn market-change
  []
  (if (> spy-relative-volume 0)
    (process-finviz-url C/DAY-TRADE-MARKET-CHANGE-UP)
    (process-finviz-url C/DAY-TRADE-MARKET-CHANGE-DOWN)))
