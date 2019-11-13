(ns db.core
  (:require
   ;; vendor
   [hikari-cp.core :refer [make-datasource close-datasource]])

  (:gen-class))

(defn as-int
  [str]
  (when str
    (Integer/parseInt str)))

(defn prop
  [s]
  (System/getProperty s))

(def datasource-options {:adapter "postgresql"
                         :username (prop "ibdb.user")
                         :password (prop "ibdb.password")
                         :database-name (prop "ibdb.name")
                         :server-name (prop "ibdb.host")
                         :port-number (as-int (prop "ibdb.port"))})

(def datasource (atom nil))

(defn init-database!
  []
  (when-not @datasource
    (reset! datasource (make-datasource datasource-options))))

(defn close-database!
  []
  (when @datasource
    (close-datasource @datasource)))

(defn get-ds
  []
  {:datasource @datasource})
