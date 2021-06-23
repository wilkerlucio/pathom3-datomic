(ns com.wsscode.pathom3.connect.datomic.client
  (:require
    [datomic.client.api :as d]))


(def client-config
  {:com.wsscode.pathom3.connect.datomic/datomic-driver-q  d/q
   :com.wsscode.pathom3.connect.datomic/datomic-driver-db d/db})
