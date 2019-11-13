(ns screener.core
  (:require
    ;; vendor
    ;; local
    [screener.day-trade :as day-trade])

  (:gen-class))

(defn day-trade
  []
  (concat
    (day-trade/main)
    (day-trade/market-change)
    (day-trade/new-high)
    (day-trade/new-low)))
