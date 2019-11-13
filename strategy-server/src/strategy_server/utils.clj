(ns strategy-server.utils)

(def client-ids (atom [0 1 2]))

(defn set-client-id
  [x]
  (swap! client-ids conj x))

(defn get-client-id
  []
  (loop [x 1]
    (if (and (some #(= x %) @client-ids)
             (< x 33))
      (recur (inc x))
      (if (< x 33) (do (set-client-id x) x) nil))))
