(ns financial-data-load.handlers
  (:require
    ;;  Vendor
    ;;  Local
    [log.core :as log]))

(defn log-error
  [{id :id
    error-code :error-code
    error-msg :error-msg}]
  (when error-code
    (log/info "error" (str "Code: " error-code) (str "message: " error-msg))))