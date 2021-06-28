(ns com.wsscode.pathom3.connect.datomic
  (:require
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [clojure.walk :as walk]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [edn-query-language.core :as eql]))

(s/def ::db any?)
(s/def ::schema (s/map-of ::p.attr/attribute map?))
(s/def ::schema-keys (s/coll-of ::p.attr/attribute :kind set?))
(s/def ::schema-uniques ::schema-keys)
(s/def ::ident-attributes ::schema-keys)

(s/def ::schema-entry
  (s/keys
    :req [:db/ident :db/id :db/valueType :db/cardinality]
    :opt [:db/doc :db/unique]))

(s/def :db/ident keyword?)
(s/def ::schema (s/map-of :db/ident ::schema-entry))

(defn datomic-entity-attribute [{:keys [::pco/op-name]}]
  (keyword (namespace op-name) "datomic-entity"))

(defn raw-datomic-q [{::keys [datomic-driver-q]} & args]
  (apply datomic-driver-q args))

(defn raw-datomic-db [{::keys [datomic-driver-db]} conn]
  (datomic-driver-db conn))

(defn db->schema
  "Extracts the schema from a Datomic db."
  [env db]
  (->> (raw-datomic-q env '[:find (pull ?e [* {:db/valueType [:db/ident]}
                                            {:db/cardinality [:db/ident]}])
                            :where
                            [_ :db.install/attribute ?e]
                            [?e :db/ident ?ident]]
                      db)
       (map first)
       (reduce
         (fn [schema entry]
           (assoc schema (:db/ident entry) entry))
         {})))

(defn schema->uniques
  "Return a set with the ident of the unique attributes in the schema."
  [schema]
  (->> schema
       vals
       (filter :db/unique)
       (into #{} (map :db/ident))))

(def registry
  [(pbir/single-attr-with-env-resolver ::conn ::db raw-datomic-db)
   (pbir/single-attr-with-env-resolver ::db ::schema db->schema)
   (pbir/single-attr-resolver ::schema ::schema-keys #(into #{:db/id} (keys %)))
   (pbir/single-attr-resolver ::schema ::schema-uniques schema->uniques)
   (pbir/constantly-resolver ::ident-attributes #{})])

(def config-env
  (-> (pci/register registry)))

(defn smart-config
  "Fulfill missing configuration options using inferences."
  [config]
  (psm/smart-map (merge config-env config) config))

(defn- prop [k]
  {:type :prop :dispatch-key k :key k})

(defn inject-ident-subqueries
  "When working with ident fields, datomic returns a reference type, but its common to
  want just the value instead. To help, you can tell Pathom which attributes you want
  to automatically pull the identity from, this will affect the queries (to include the
  :db/ident as part of the request) and will pull that in post-process."
  [{::keys [ident-attributes]} ast]
  (->> ast
       (eql/transduce-children
         (map (fn [{:keys [key query] :as ast}]
                (if (and (contains? ident-attributes key) (not query))
                  (assoc ast :type :join :query [:db/ident] :children [(prop :db/ident)])
                  ast))))
       eql/ast->query))

(defn pick-ident-key
  "Figures which key to use to request data from Datomic. This will
  try to pick :db/id if available, returning the number directly.
  Otherwise will look for some attribute that is a unique and is on
  the map, in case of multiple one will be selected by random. The
  format of the unique return is [:attribute value]."
  [{::keys [schema-uniques admin-mode?]} m]
  (if (and (contains? m :db/id) admin-mode?)
    (:db/id m)

    (let [available (set/intersection schema-uniques (into #{} (keys m)))]
      (if-let [attr (first available)]
        [attr (get m attr)]))))

(defn post-process-entity
  "Post process the result from the datomic query. Operations that it does:
  - Pull :db/ident from ident fields"
  [{::keys [ident-attributes]} entity]
  (walk/postwalk
    (fn [x]
      (if (and (map-entry? x)
               (contains? ident-attributes (key x))
               (contains? (val x) :db/ident))
        (coll/make-map-entry (key x) (:db/ident (val x)))
        x))
    entity))

(defn datomic-resolve
  "Runs the resolver to fetch Datomic data from identities."
  [config
   {::keys [db]
    :as    env}
   input]
  (let [id          (pick-ident-key config input)
        foreign-ast (-> env ::pcp/node ::pcp/foreign-ast)]
    (cond
      (nil? id) nil

      (integer? id)
      (->> (raw-datomic-q config [:find (list 'pull '?e (inject-ident-subqueries config foreign-ast))
                                  :in '$ '?e]
                          db id)
           ffirst
           (post-process-entity config))

      (eql/ident? id)
      (let [[k v] id]
        (->> (raw-datomic-q config [:find (list 'pull '?e (inject-ident-subqueries config foreign-ast))
                                    :in '$ '?v
                                    :where ['?e k '?v]]
                            db
                            v)
             ffirst
             (post-process-entity config))))))

; region query helpers

(defn datomic-subquery-q
  "Compute nested sub-query using the planner capabilities and run the query on Datomic
  pulling the necessary data out."
  [{::keys [db] ::pcp/keys [node graph] :as env} {:keys [::pco/op-name] ::p.attr/keys [attribute]} query]
  (let [attr    (or attribute (-> node ::pcp/expects ffirst))
        ast     (get-in graph [::pcp/index-ast attr])
        sub-ast (-> (pcp/compute-dynamic-resolver-nested-requirements
                      (assoc env :edn-query-language.ast/node ast
                        ::pco/dynamic-name op-name
                        ::p.attr/attribute attr
                        ::pco/op-name (::pco/op-name node)))
                    (pfsd/shape-descriptor->ast))
        config  (get-in env [::datomic-config op-name])]
    (->> (raw-datomic-q config (assoc query :find [(list 'pull '?e (inject-ident-subqueries config sub-ast))])
                        (or db (raw-datomic-db config (::conn config))))
         (map (comp #(post-process-entity config %) #(or % {}) first)))))

(defn query-entities
  "Use this helper from inside a resolver to run a Datomic query.
  You must send dquery using a datalog map format. The :find section
  of the query will be populated by this function with [[pull ?e SUB_QUERY] '...].
  The SUB_QUERY will be computed by Pathom, considering the current user sub-query.
  Example resolver (using Datomic mbrainz sample database):
      (pco/defresolver artists-before-1600 [env _]
        {::pco/output [{:artist/artists-before-1600 [:db/id]}]}
        {:artist/artists-before-1600
         (pcd/query-entities env
           '{:where [[?e :artist/name ?name]
                     [?e :artist/startYear ?year]
                     [(< ?year 1600)]]})})
  Notice the result binding entities must be named as `?e`.
  Them the user can run queries like:
      [{:artist/artists-before-1600
        [:artist/name
         :not-in/datomic
         {:artist/country
          [:country/name]}]}]
  The sub-query will be send to Datomic, filtering out unsupported keys
  like `:not-in/datomic`."
  [env datomic-source dquery]
  (vec (datomic-subquery-q env datomic-source dquery)))

(defn query-entity
  "Like query-entities, but returns a single result. This leverage Datomic
  single result :find, meaning it is effectively more efficient than query-entities."
  [env datomic-source dquery]
  (first (datomic-subquery-q env datomic-source dquery)))

(defn make-resolver [op-name config output-attr f-or-query query-fn]
  (pco/resolver op-name
    (merge
      (select-keys config [::pco/input ::pco/params])
      {::pco/output [{output-attr [(datomic-entity-attribute config)]}]})
    (fn [env input]
      {output-attr
       (query-fn env
                 (assoc config ::p.attr/attribute output-attr)
                 (if (fn? f-or-query)
                   (f-or-query env input)
                   f-or-query))})))

(defn entity-resolver [op-name config output-attr f-or-query]
  (make-resolver op-name config output-attr f-or-query query-entity))

(defn entities-resolver [op-name config output-attr f-or-query]
  (make-resolver op-name config output-attr f-or-query query-entities))

; endregion

(defn ref-attribute? [{::keys [schema]} attr]
  (= :db.type/ref (get-in schema [attr :db/valueType :db/ident])))

(defn index-schema
  "Creates Pathom index from Datomic schema."
  [{::keys     [schema schema-uniques admin-mode?]
    ::pco/keys [op-name] :as config}]
  (let [entity-attr     (datomic-entity-attribute config)
        schema-output   (into []
                              (comp
                                (remove (comp #(some->> % namespace (re-find #"^db\.?"))))
                                (map (fn [field]
                                       (if (ref-attribute? config field)
                                         {field [entity-attr]}
                                         field))))
                              (keys schema))
        entity-resolver (pco/resolver (symbol (namespace op-name) "datomic-entity-resolver")
                          {::pco/dynamic-name op-name
                           ::pco/input        [entity-attr]
                           ::pco/output       schema-output})]
    (pci/register
      {::pci/transient-attrs entity-attr}
      (cond-> [(pco/resolver {::datomic?              true
                              ::pco/op-name           op-name
                              ::pco/cache?            false
                              ::pco/dynamic-resolver? true
                              ::pco/resolve           (fn datomic-resolve-internal [env input]
                                                        (datomic-resolve config
                                                                         (merge {::db (raw-datomic-db config (::conn config))}
                                                                                env)
                                                                         input))})
               entity-resolver]

        admin-mode?
        (into
          (map
            (fn [attr]
              (let [resolver-name (symbol (namespace op-name) (str "datomic-ident-" (pbir/attr-munge attr)))]
                (pco/resolver resolver-name
                  {::pco/dynamic-name op-name
                   ::pco/input        [attr]
                   ::pco/output       [entity-attr]}))))
          (conj schema-uniques :db/id))))))

(>defn connect-datomic
  "Plugin to add datomic integration.
  Options:

  ::pco/op-name (required) - a unique identifier for this connection, as a symbol
  ::conn (required) - Datomic connection
  ::ident-attributes - a set containing the attributes to be treated as idents
  ::admin-mode? - boolean to set admin mode, in this mode it allows full access to
  any datomic entity via :db/id or via some ident attribute. Note this is mode is not
  secure for most applications, instead you should add manual access points so you
  can make them secure. Check the docs for more details.
  ::db - Datomic db, if not provided will be computed from ::conn
  "
  [env {:keys [::pco/op-name] :as config}]
  [map? (s/keys :req [::pco/op-name ::conn] :opt [::ident-attributes]) => map?]
  (let [config'       (smart-config config)
        datomic-index (index-schema config')]
    (-> env
        (assoc-in [::datomic-config op-name] config)
        (pci/register datomic-index))))
