(ns financial-data-load.load-financial-data
  (:require
    ;;vendor
    [clojure.data.xml :refer :all]
    [clojure.data.json :as json]
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    ;;local
    [financial-data-load.utils :refer [get-contract-details-params create-contract]]
    [financial-data-load.database.pull-from-db :as db-pull]
    [financial-data-load.database.push-to-db :as db-push]
    [financial-data-load.database.deletes :as db-del]
    [ib-client.utils :as client-utils]))

(defn different-keys? [content]
  (when content
    (let [dkeys (count (filter identity (distinct (map :tag content))))
          n (count content)]
      (= dkeys n))))

(defn to-map [element]
  (cond
    (nil? element) nil
    (string? element) element
    (sequential? element) (if (> (count element) 1)
                            (if (different-keys? element)
                              (reduce into {} (map (partial to-map ) element))
                              (map to-map element))
                            (to-map  (first element)))
    (and (map? element) (empty? element)) {}
    (map? element) (if (:attrs element)
                     {(:tag element) (to-map (:content element))
                      (keyword (str (name (:tag element)) "Attrs")) (:attrs element)}
                     {(:tag element) (to-map  (:content element))})
    :else nil))

(defn load-estimates-data
  [api]
  (doall
    (pmap
      (fn [ticker]
        (if-let [contract-details (get-contract-details-params api ticker)]
          (if-let [estimates-data (client-utils/request-ticker-fundamental-data
                                    api (.contract contract-details) "RESC")]
            (db-push/update-financial-data
              ticker
              {:date (c/to-sql-time (t/now))
               :estimates
                     (json/write-str
                       (to-map (parse (java.io.StringReader.(:xml estimates-data)))))})
            (db-del/remove-from-estimates-data ticker))
          (db-del/remove-from-estimates-data ticker)))
      (db-pull/pull-valid-tickers)))
  (db-del/remove-nil-estimates))
