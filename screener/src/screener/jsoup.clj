(ns screener.jsoup
  (:import
    [org.jsoup Jsoup]))

(defn get-page
  [url]
  (.get (Jsoup/connect url)))