(ns historical-data-load.handlers
  (:require
   ;;  Vendor
   [log.core :as log]))
   ;;  Local

(defn log-error
  [{id :id
    error-code :error-code
    error-msg :error-msg}]
  (when error-code
    (log/info "error" (str "Code: " error-code) (str "message: " error-msg))))