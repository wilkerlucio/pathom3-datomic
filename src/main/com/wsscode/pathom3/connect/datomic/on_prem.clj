(ns com.wsscode.pathom3.connect.datomic.on-prem
  (:require
    [datomic.api :as d]))


(def on-prem-config
  {:com.wsscode.pathom3.connect.datomic/datomic-driver-q  d/q
   :com.wsscode.pathom3.connect.datomic/datomic-driver-db d/db})
