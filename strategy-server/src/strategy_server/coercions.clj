(ns strategy-server.coercions)

(defn as-int32
  "Parse a string into an int32, or `nil` if the string cannot be parsed."
  [s]
  (try
    (Integer/parseInt s)
    (catch NumberFormatException _ nil)))

(defn as-float
  "Parse a string into an float, or `nil` if the string cannot be parsed."
  [s]
  (try
    (Float/parseFloat s)
    (catch NumberFormatException _ nil)))