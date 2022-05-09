(ns com.wsscode.pathom3.connect.datomic-devlocal-test
  (:require
    [clojure.test :as t :refer :all]
    [com.wsscode.pathom3.connect.built-in.plugins :as pbip]
    [com.wsscode.pathom3.connect.datomic :as pcd]
    [com.wsscode.pathom3.connect.datomic.client :refer [client-config]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [datomic.client.api :as d]))

;;
;; Setup
;; 1) Install Datomic dev-local (https://docs.datomic.com/cloud/dev-local.html#getting-started)
;; 2) Install sample data (https://docs.datomic.com/cloud/dev-local.html#samples)
;;

(def client (d/client {:server-type :dev-local
                       :system "datomic-samples"}))
(def datomic-config {:db-name "mbrainz-subset"})
(d/create-database client datomic-config)
(def conn (d/connect client datomic-config))
(def db (d/db conn))

(def db-config
  (assoc client-config
         ::pco/op-name `mbrainz))

(def registry
  [])

(def env-admin
  (-> (pci/register
        [registry])
      (pcd/connect-datomic
        (assoc db-config
               ::pcd/conn conn
               ::pcd/admin-mode? true
               ::pcd/ident-attributes #{:artist/type}))
      (p.plugin/register (pbip/attribute-errors-plugin))))

(def request-admin (p.eql/boundary-interface env-admin))

(deftest datomic-admin-mode-test
  (testing "reading from :db/id"
    (let [janis-eid  (ffirst (d/q '[:find ?id :where [?id :artist/name "Janis Joplin"]] db))]
         (is (= (request-admin [{[:db/id janis-eid] [:artist/name]}])
                {[:db/id janis-eid] {:artist/name "Janis Joplin"}})))))