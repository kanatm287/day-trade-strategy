(ns back-test.database.pull-from-db
  (:require
   ;; vendor
   [clojure.java.jdbc :as j]
   [honeysql.core :as sql]
   ;; local
   [db.core :as db]))

(defn pull-last-test-date
  [ticker time-frame strategy action]
  (j/query (db/get-ds)
    (sql/format {:select [:date]
                 :from [:test_params]
                 :where [:and
                         [:= :symbol ticker]
                         [:= :time_frame time-frame]
                         [:= :strategy strategy]
                         [:= :action action]]})
    {:row-fn (fn [e] (:date e))
     :result-set-fn first}))

(defn pull-back-test-return
  [ticker time-frame strategy action column]
  (let [column-name (if (or
                          (= column "max_drawdown")
                          (= column "beta"))
                      column
                      (str column "_period_return"))]
    ((keyword column-name)
     (j/query (db/get-ds)
       (sql/format {:select [(keyword column-name)]
                    :from [:test_params]
                    :where [:and
                            [:= :symbol ticker]
                            [:= :time_frame time-frame]
                            [:= :strategy strategy]
                            [:= :action action]]})
       {:result-set-fn first}))))

(defn pull-back-test-value
  [ticker time-frame strategy action column]
  (let [column-name (str "portfolio_value" column)]
    ((keyword column-name)
     (j/query (db/get-ds)
       (sql/format {:select [(keyword column-name)]
                    :from [:test_params]
                    :where [:and
                            [:= :symbol ticker]
                            [:= :time_frame time-frame]
                            [:= :strategy strategy]
                            [:= :action action]]})
       {:result-set-fn first}))))

(defn pull-minute-nil-ticker
  [action]
  (first
    (j/query (db/get-ds)
      (sql/format {:select [:symbol]
                   :modifiers [:distinct]
                   :from [:test_params]
                   :where [:and
                           [:not-in :symbol {:select [:ticker]
                                             :from [:illiquid_list]}]
                           [:not-in :symbol {:select [:ticker]
                                             :modifiers [:distinct]
                                             :from [:back_test_error]}]
                           [:= :combined_period_return nil]
                           [:= :action action]
                           [:= :time_frame "minute"]]
                   :order-by [[:symbol :asc]]
                   :limit 1})
      {:row-fn :symbol})))

(defn pull-back-test-error-minute-tickers
  []
  (j/query (db/get-ds)
    (sql/format {:select [:ticker]
                 :modifiers [:distinct]
                 :from [:back_test_error]
                 :where [:or
                         [:= :strategy "trend"]
                         [:= :strategy "correction"]
                         [:= :test_param "combined"]]})
    {:row-fn :ticker}))
