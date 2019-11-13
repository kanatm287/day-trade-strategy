(ns thin-client.strat.day-trade
  (:require
    ;; vendor
    ;; local
   [thin-client.strat.core :as strat]))

(defn process-instructions
  [instructions]
  (when instructions
    (strat/process-instructions instructions)))
