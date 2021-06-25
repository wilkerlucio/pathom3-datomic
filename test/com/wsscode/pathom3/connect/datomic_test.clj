(ns com.wsscode.pathom3.connect.datomic-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.plugins :as pbip]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.datomic :as pcd]
    [com.wsscode.pathom3.connect.datomic.on-prem :refer [on-prem-config]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [datomic.api :as d]
    [edn-query-language.core :as eql]))

(def uri "datomic:free://localhost:4334/mbrainz-1968-1973")
(def conn (d/connect uri))

(def db (d/db conn))

(def db-config
  (assoc on-prem-config
    ::pcd/dynamic-op-name `mbrainz
    ::pcd/allow-list ::pcd/DANGER_ALLOW_ALL!))

(def allow-list
  #{:artist/country
    :artist/gid
    :artist/name
    :artist/sortName
    :artist/type
    :country/name
    :medium/format
    :medium/name
    :medium/position
    :medium/trackCount
    :medium/tracks
    :release/artists
    :release/country
    :release/day
    :release/gid
    :release/labels
    :release/language
    :release/media
    :release/month
    :release/name
    :release/packaging
    :release/script
    :release/status
    :release/year
    :track/artists
    :track/duration
    :track/name
    :track/position})

(def db-schema-output
  {:abstractRelease/artistCredit #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The string represenation of the artist(s) to be credited on the abstract release"
                                      :fulltext    true
                                      :id          82
                                      :ident       :abstractRelease/artistCredit
                                      :valueType   #:db{:ident :db.type/string}}
   :abstractRelease/artists      #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The set of artists contributing to the abstract release"
                                      :id          81
                                      :ident       :abstractRelease/artists
                                      :valueType   #:db{:ident :db.type/ref}}
   :abstractRelease/gid          #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for the abstract release"
                                      :id          78
                                      :ident       :abstractRelease/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :abstractRelease/name         #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the abstract release"
                                      :id          79
                                      :ident       :abstractRelease/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :abstractRelease/type         #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one
  of: :release.type/album, :release.type/single, :release.type/ep, :release.type/audiobook,
  or :release.type/other"
                                      :id          80
                                      :ident       :abstractRelease/type
                                      :valueType   #:db{:ident :db.type/ref}}
   :artist/country               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artist's country of origin"
                                      :id          71
                                      :ident       :artist/country
                                      :valueType   #:db{:ident :db.type/ref}}
   :artist/endDay                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the artist stopped actively recording"
                                      :id          77
                                      :ident       :artist/endDay
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/endMonth              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the artist stopped actively recording"
                                      :id          76
                                      :ident       :artist/endMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/endYear               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the artist stopped actively recording"
                                      :id          75
                                      :ident       :artist/endYear
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/gender                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one of :artist.gender/male, :artist.gender/female, or :artist.gender/other."
                                      :id          70
                                      :ident       :artist/gender
                                      :valueType   #:db{:ident :db.type/ref}}
   :artist/gid                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for an artist"
                                      :id          66
                                      :ident       :artist/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :artist/name                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artist's name"
                                      :fulltext    true
                                      :id          67
                                      :ident       :artist/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :artist/sortName              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artist's name for use in alphabetical sorting, e.g. Beatles, The"
                                      :id          68
                                      :ident       :artist/sortName
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :artist/startDay              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the artist started actively recording"
                                      :id          74
                                      :ident       :artist/startDay
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/startMonth            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the artist started actively recording"
                                      :id          73
                                      :ident       :artist/startMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/startYear             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the artist started actively recording"
                                      :id          72
                                      :ident       :artist/startYear
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :artist/type                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one of :artist.type/person, :artist.type/other, :artist.type/group."
                                      :id          69
                                      :ident       :artist/type
                                      :valueType   #:db{:ident :db.type/ref}}
   :country/name                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the country"
                                      :id          63
                                      :ident       :country/name
                                      :unique      #:db{:id 37}
                                      :valueType   #:db{:ident :db.type/string}}
   :db.alter/attribute           #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will alter the definition of existing attribute v."
                                      :id          19
                                      :ident       :db.alter/attribute
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.excise/attrs              #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :id          16
                                      :ident       :db.excise/attrs
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.excise/before             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :id          18
                                      :ident       :db.excise/before
                                      :valueType   #:db{:ident :db.type/instant}}
   :db.excise/beforeT            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :id          17
                                      :ident       :db.excise/beforeT
                                      :valueType   #:db{:ident :db.type/long}}
   :db.install/attribute         #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as an attribute."
                                      :id          13
                                      :ident       :db.install/attribute
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.install/function          #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as a data function."
                                      :id          14
                                      :ident       :db.install/function
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.install/partition         #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as a partition."
                                      :id          11
                                      :ident       :db.install/partition
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.install/valueType         #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "System attribute with type :db.type/ref. Asserting this attribute on :db.part/db with value v will install v as a value type."
                                      :id          12
                                      :ident       :db.install/valueType
                                      :valueType   #:db{:ident :db.type/ref}}
   :db.sys/partiallyIndexed      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "System-assigned attribute set to true for transactions not fully incorporated into the index"
                                      :id          8
                                      :ident       :db.sys/partiallyIndexed
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db.sys/reId                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "System-assigned attribute for an id e in the log that has been changed to id v in the index"
                                      :id          9
                                      :ident       :db.sys/reId
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/cardinality               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. Two possible values: :db.cardinality/one for single-valued attributes, and :db.cardinality/many for many-valued attributes. Defaults to :db.cardinality/one."
                                      :id          41
                                      :ident       :db/cardinality
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/code                      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "String-valued attribute of a data function that contains the function's source code."
                                      :fulltext    true
                                      :id          47
                                      :ident       :db/code
                                      :valueType   #:db{:ident :db.type/string}}
   :db/doc                       #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Documentation string for an entity."
                                      :fulltext    true
                                      :id          62
                                      :ident       :db/doc
                                      :valueType   #:db{:ident :db.type/string}}
   :db/excise                    #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :id          15
                                      :ident       :db/excise
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/fn                        #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "A function-valued attribute for direct use by transactions and queries."
                                      :id          52
                                      :ident       :db/fn
                                      :valueType   #:db{:ident :db.type/fn}}
   :db/fulltext                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If true, create a fulltext search index for the attribute. Defaults to false."
                                      :id          51
                                      :ident       :db/fulltext
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/ident                     #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Attribute used to uniquely name an entity."
                                      :id          10
                                      :ident       :db/ident
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/keyword}}
   :db/index                     #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If true, create an AVET index for the attribute. Defaults to false."
                                      :id          44
                                      :ident       :db/index
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/isComponent               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of attribute whose vtype is :db.type/ref. If true, then the attribute is a component of the entity referencing it. When you query for an entire entity, components are fetched automatically. Defaults to nil."
                                      :id          43
                                      :ident       :db/isComponent
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/lang                      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Attribute of a data function. Value is a keyword naming the implementation language of the function. Legal values are :db.lang/java and :db.lang/clojure"
                                      :id          46
                                      :ident       :db/lang
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/noHistory                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If true, past values of the attribute are not retained after indexing. Defaults to false."
                                      :id          45
                                      :ident       :db/noHistory
                                      :valueType   #:db{:ident :db.type/boolean}}
   :db/txInstant                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Attribute whose value is a :db.type/instant. A :db/txInstant is recorded automatically with every transaction."
                                      :id          50
                                      :ident       :db/txInstant
                                      :index       true
                                      :valueType   #:db{:ident :db.type/instant}}
   :db/unique                    #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute. If value is :db.unique/value, then attribute value is unique to each entity. Attempts to insert a duplicate value for a temporary entity id will fail. If value is :db.unique/identity, then attribute value is unique, and upsert is enabled. Attempting to insert a duplicate value for a temporary entity id will cause all attributes associated with that temporary id to be merged with the entity already in the database. Defaults to nil."
                                      :id          42
                                      :ident       :db/unique
                                      :valueType   #:db{:ident :db.type/ref}}
   :db/valueType                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Property of an attribute that specifies the attribute's value type. Built-in value types include, :db.type/keyword, :db.type/string, :db.type/ref, :db.type/instant, :db.type/long, :db.type/bigdec, :db.type/boolean, :db.type/float, :db.type/uuid, :db.type/double, :db.type/bigint,  :db.type/uri."
                                      :id          40
                                      :ident       :db/valueType
                                      :valueType   #:db{:ident :db.type/ref}}
   :fressian/tag                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Keyword-valued attribute of a value type that specifies the underlying fressian type used for serialization."
                                      :id          39
                                      :ident       :fressian/tag
                                      :index       true
                                      :valueType   #:db{:ident :db.type/keyword}}
   :label/country                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The country where the record label is located"
                                      :id          87
                                      :ident       :label/country
                                      :valueType   #:db{:ident :db.type/ref}}
   :label/endDay                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the label stopped business"
                                      :id          93
                                      :ident       :label/endDay
                                      :valueType   #:db{:ident :db.type/long}}
   :label/endMonth               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the label stopped business"
                                      :id          92
                                      :ident       :label/endMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :label/endYear                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the label stopped business"
                                      :id          91
                                      :ident       :label/endYear
                                      :valueType   #:db{:ident :db.type/long}}
   :label/gid                    #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for the record label"
                                      :id          83
                                      :ident       :label/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :label/name                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the record label"
                                      :fulltext    true
                                      :id          84
                                      :ident       :label/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :label/sortName               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the record label for use in alphabetical sorting"
                                      :id          85
                                      :ident       :label/sortName
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :label/startDay               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day the label started business"
                                      :id          90
                                      :ident       :label/startDay
                                      :valueType   #:db{:ident :db.type/long}}
   :label/startMonth             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month the label started business"
                                      :id          89
                                      :ident       :label/startMonth
                                      :valueType   #:db{:ident :db.type/long}}
   :label/startYear              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year the label started business"
                                      :id          88
                                      :ident       :label/startYear
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :label/type                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Enum, one of :label.type/distributor, :label.type/holding,
  :label.type/production, :label.type/originalProduction,
  :label.type/bootlegProduction, :label.type/reissueProduction, or
  :label.type/publisher."
                                      :id          86
                                      :ident       :label/type
                                      :valueType   #:db{:ident :db.type/ref}}
   :language/name                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the written and spoken language"
                                      :id          64
                                      :ident       :language/name
                                      :unique      #:db{:id 37}
                                      :valueType   #:db{:ident :db.type/string}}
   :medium/format                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The format of the medium. An enum with lots of possible values"
                                      :id          111
                                      :ident       :medium/format
                                      :valueType   #:db{:ident :db.type/ref}}
   :medium/name                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the medium itself, distinct from the name of the release"
                                      :fulltext    true
                                      :id          113
                                      :ident       :medium/name
                                      :valueType   #:db{:ident :db.type/string}}
   :medium/position              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The position of this medium in the release relative to the other media, i.e. disc 1"
                                      :id          112
                                      :ident       :medium/position
                                      :valueType   #:db{:ident :db.type/long}}
   :medium/trackCount            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The total number of tracks on the medium"
                                      :id          114
                                      :ident       :medium/trackCount
                                      :valueType   #:db{:ident :db.type/long}}
   :medium/tracks                #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The set of tracks found on this medium"
                                      :id          110
                                      :ident       :medium/tracks
                                      :isComponent true
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/abstractRelease      #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "This release is the physical manifestation of the
  associated abstract release, e.g. the the 1984 US vinyl release of
  \"The Wall\" by Columbia, as opposed to the 2000 US CD release of
  \"The Wall\" by Capitol Records."
                                      :id          108
                                      :ident       :release/abstractRelease
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/artistCredit         #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The string represenation of the artist(s) to be credited on the release"
                                      :fulltext    true
                                      :id          106
                                      :ident       :release/artistCredit
                                      :valueType   #:db{:ident :db.type/string}}
   :release/artists              #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The set of artists contributing to the release"
                                      :id          107
                                      :ident       :release/artists
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/barcode              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The barcode on the release packaging"
                                      :id          99
                                      :ident       :release/barcode
                                      :valueType   #:db{:ident :db.type/string}}
   :release/country              #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The country where the recording was released"
                                      :id          95
                                      :ident       :release/country
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/day                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The day of the release"
                                      :id          105
                                      :ident       :release/day
                                      :valueType   #:db{:ident :db.type/long}}
   :release/gid                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The globally unique MusicBrainz ID for the release"
                                      :id          94
                                      :ident       :release/gid
                                      :unique      #:db{:id 38}
                                      :valueType   #:db{:ident :db.type/uuid}}
   :release/labels               #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The label on which the recording was released"
                                      :id          96
                                      :ident       :release/labels
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/language             #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The language used in the release"
                                      :id          98
                                      :ident       :release/language
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/media                #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The various media (CDs, vinyl records, cassette tapes, etc.) included in the release."
                                      :id          101
                                      :ident       :release/media
                                      :isComponent true
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/month                #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The month of the release"
                                      :id          104
                                      :ident       :release/month
                                      :valueType   #:db{:ident :db.type/long}}
   :release/name                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The name of the release"
                                      :fulltext    true
                                      :id          100
                                      :ident       :release/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :release/packaging            #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The type of packaging used in the release, an enum, one
  of: :release.packaging/jewelCase, :release.packaging/slimJewelCase, :release.packaging/digipak, :release.packaging/other
  , :release.packaging/keepCase, :release.packaging/none,
  or :release.packaging/cardboardPaperSleeve"
                                      :id          102
                                      :ident       :release/packaging
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/script               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The script used in the release"
                                      :id          97
                                      :ident       :release/script
                                      :valueType   #:db{:ident :db.type/ref}}
   :release/status               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The status of the release"
                                      :id          109
                                      :ident       :release/status
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :release/year                 #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The year of the release"
                                      :id          103
                                      :ident       :release/year
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :script/name                  #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "Name of written character set, e.g. Hebrew, Latin, Cyrillic"
                                      :id          65
                                      :ident       :script/name
                                      :unique      #:db{:id 37}
                                      :valueType   #:db{:ident :db.type/string}}
   :track/artistCredit           #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The artists who contributed to the track"
                                      :fulltext    true
                                      :id          116
                                      :ident       :track/artistCredit
                                      :valueType   #:db{:ident :db.type/string}}
   :track/artists                #:db{:cardinality #:db{:ident :db.cardinality/many}
                                      :doc         "The artists who contributed to the track"
                                      :id          115
                                      :ident       :track/artists
                                      :valueType   #:db{:ident :db.type/ref}}
   :track/duration               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The duration of the track in msecs"
                                      :id          119
                                      :ident       :track/duration
                                      :index       true
                                      :valueType   #:db{:ident :db.type/long}}
   :track/name                   #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The track name"
                                      :fulltext    true
                                      :id          118
                                      :ident       :track/name
                                      :index       true
                                      :valueType   #:db{:ident :db.type/string}}
   :track/position               #:db{:cardinality #:db{:ident :db.cardinality/one}
                                      :doc         "The position of the track relative to the other tracks on the medium"
                                      :id          117
                                      :ident       :track/position
                                      :valueType   #:db{:ident :db.type/long}}})

(deftest test-db->schema
  (is (= (pcd/db->schema db-config db)
         db-schema-output)))

(deftest test-schema->uniques
  (is (= (pcd/schema->uniques db-schema-output)
         #{:abstractRelease/gid
           :artist/gid
           :country/name
           :db/ident
           :label/gid
           :language/name
           :release/gid
           :script/name})))

(deftest test-inject-ident-subqueries
  (testing "add ident sub query part on ident fields"
    (is (= (pcd/inject-ident-subqueries
             {::pcd/ident-attributes #{:foo}
              ::pcd/allow-list       ::pcd/DANGER_ALLOW_ALL!}
             (eql/query->ast [:foo]))
           [{:foo [:db/ident]}]))))

(deftest test-pick-ident-key
  (let [config (pcd/smart-config (merge db-config {::pcd/conn conn}))]
    (testing "nothing available"
      (is (= (pcd/pick-ident-key config
                                 {})
             nil))
      (is (= (pcd/pick-ident-key config
                                 {:id  123
                                  :foo "bar"})
             nil)))
    (testing "pick from :db/id"
      (is (= (pcd/pick-ident-key config
                                 {:db/id 123})
             123)))
    (testing "picking from schema unique"
      (is (= (pcd/pick-ident-key config
                                 {:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"})
             [:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"])))
    (testing "prefer :db/id"
      (is (= (pcd/pick-ident-key config
                                 {:db/id      123
                                  :artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"})
             123)))))

(def index-io-secure-output
  {#{:artist/gid}   {:db/id {}}
   #{:country/name} {:db/id {}}
   #{:db/id}        {:artist/country    {:db/id {}}
                     :artist/gid        {}
                     :artist/name       {}
                     :artist/sortName   {}
                     :artist/type       {:db/id {}}
                     :country/name      {}
                     :medium/format     {:db/id {}}
                     :medium/name       {}
                     :medium/position   {}
                     :medium/trackCount {}
                     :medium/tracks     {:db/id {}}
                     :release/artists   {:db/id {}}
                     :release/country   {:db/id {}}
                     :release/day       {}
                     :release/gid       {}
                     :release/labels    {:db/id {}}
                     :release/language  {:db/id {}}
                     :release/media     {:db/id {}}
                     :release/month     {}
                     :release/name      {}
                     :release/packaging {:db/id {}}
                     :release/script    {:db/id {}}
                     :release/status    {}
                     :release/year      {}
                     :track/artists     {:db/id {}}
                     :track/duration    {}
                     :track/name        {}
                     :track/position    {}}
   #{:release/gid}  {:db/id {}}})

(def index-idents-secure-output
  #{:artist/gid
    :country/name
    :release/gid})

(deftest test-index-schema-secure
  (let [index (-> (pcd/smart-config
                    (assoc db-config
                      ::pcd/conn conn
                      ::pcd/allow-list allow-list))
                  (pcd/index-schema)
                  (pci/register))]
    (is (= (::pci/index-oir index)
           '{:artist/country    {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :artist/gid        {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :artist/name       {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :artist/sortName   {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :artist/type       {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :country/name      {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :db/id             {{:artist/gid {}}   #{com.wsscode.pathom3.connect.datomic-test/datomic-ident-artist_SLASH_gid}
                                 {:country/name {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-ident-country_SLASH_name}
                                 {:release/gid {}}  #{com.wsscode.pathom3.connect.datomic-test/datomic-ident-release_SLASH_gid}}
             :medium/format     {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :medium/name       {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :medium/position   {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :medium/trackCount {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :medium/tracks     {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/artists   {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/country   {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/day       {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/gid       {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/labels    {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/language  {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/media     {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/month     {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/name      {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/packaging {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/script    {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/status    {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :release/year      {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :track/artists     {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :track/duration    {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :track/name        {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}
             :track/position    {{:db/id {}} #{com.wsscode.pathom3.connect.datomic-test/datomic-entity-resolver}}}))

    (is (= (::pci/index-io index)
           index-io-secure-output))))

(deftest post-process-entity-test
  (is (= (pcd/post-process-entity
           {::pcd/ident-attributes #{:artist/type}}
           {:artist/type {:db/ident :artist.type/person}})
         {:artist/type :artist.type/person})))

(def super-name
  (pbir/single-attr-resolver :artist/name :artist/super-name #(str "SUPER - " %)))

(pco/defresolver years-active [{:artist/keys [startYear endYear]}]
  {:artist/active-years-count (- endYear startYear)})

(pco/defresolver artists-before-1600 [env _]
  {::pco/output [{:artist/artists-before-1600 [:db/id]}]}
  {:artist/artists-before-1600
   (pcd/query-entities env
                       {::pcd/dynamic-op-name `mbrainz}
                       '{:where [[?e :artist/name ?name]
                                 [?e :artist/startYear ?year]
                                 [(< ?year 1600)]]})})

(pco/defresolver artist-before-1600 [env _]
  {::pco/output [{:artist/artist-before-1600 [:db/id]}]}
  {:artist/artist-before-1600
   (pcd/query-entity env
                     {::pcd/dynamic-op-name `mbrainz}
                     '{:where [[?e :artist/name ?name]
                               [?e :artist/startYear ?year]
                               [(< ?year 1600)]]})})

(pco/defresolver all-mediums [env _]
  {::pco/output [{:all-mediums [:db/id]}]}
  {:all-mediums
   (pcd/query-entities env
                       {::pcd/dynamic-op-name `mbrainz}
                       '{:where [[?e :medium/name _]]})})

(def registry
  [super-name
   years-active
   artists-before-1600
   artist-before-1600
   all-mediums])

(def env
  (-> (pci/register
        [registry])
      (pcd/connect-datomic
        (assoc db-config
          ::pcd/conn conn
          ::pcd/ident-attributes #{:artist/type}))
      (p.plugin/register (pbip/attribute-errors-plugin))))

(def request (p.eql/boundary-interface env))

(comment
  (d/q
    '[:find (pull ?e [:artist/sortName])
      :in $ ?e]
    db 756463999921184)

  (p.eql/process env
    {:db/id 756463999921184}
    [:artist/super-name
     {:artist/country [:country/name]}]))

(deftest test-datomic-parser
  (testing "reading from :db/id"
    (is (= (request
             [{[:db/id 637716744120508]
               [:artist/name]}])
           {[:db/id 637716744120508] {:artist/name "Janis Joplin"}})))

  (testing "nested"
    (is (= (request
             [{[:db/id 637716744120508]
               [:artist/name
                {:artist/country [:country/name]}]}])
           {[:db/id 637716744120508] {:artist/name    "Janis Joplin",
                                      :artist/country {:country/name "United States"}}})))

  (testing "reading from unique attribute"
    (is (= (request [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                      [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "Janis Joplin"}})))

  (testing "explicit db"
    (is (= (request {::pcd/db (:db-after (d/with (d/db conn)
                                                 [{:artist/gid  #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"
                                                   :artist/name "not Janis Joplin"}]))}
                    [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                      [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "not Janis Joplin"}})))

  (comment
    "after transact data (I will not transact in your mbrainz), parser should take a new db"
    (d/transact conn
      [{:artist/gid  #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"
        :artist/name "not Janis Joplin"}])
    (is (= (request {}
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "not Janis Joplin"}})))

  (testing "implicit dependency"
    (is (= (request {}
                    [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                      [:artist/super-name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/super-name "SUPER - Janis Joplin"}})))

  (testing "process-query"
    (is (= (request [{:artist/artists-before-1600
                      [:artist/super-name
                       {:artist/country
                        [:country/name]}]}])
           {:artist/artists-before-1600
            [{:artist/super-name "SUPER - Heinrich Schütz",
              :artist/country    {:country/name "Germany"}}
             {:artist/super-name "SUPER - Choir of King's College, Cambridge",
              :artist/country    {:country/name "United Kingdom"}}]}))

    (is (= (request {}
                    [{:artist/artist-before-1600
                      [:artist/super-name
                       {:artist/country
                        [:country/name
                         :db/id]}]}])
           {:artist/artist-before-1600
            {:artist/super-name "SUPER - Heinrich Schütz",
             :artist/country    {:country/name "Germany"
                                 :db/id        17592186045657}}}))

    (testing "partial missing information on entities"
      (is (= (request {::pcd/db (-> (d/with db
                                            [{:medium/name "val"}
                                             {:medium/name "6val"
                                              :artist/name "bla"}
                                             {:medium/name "3"
                                              :artist/name "bar"}])
                                    :db-after)}
                      [{:all-mediums
                        [:artist/name :medium/name]}])
             {:all-mediums [{:artist/name "bar", :medium/name "3"}
                            {:medium/name                                         "val",
                             :com.wsscode.pathom3.connect.runner/attribute-errors {:artist/name {:com.wsscode.pathom3.error/error-type :com.wsscode.pathom3.error/attribute-unreachable}}}
                            {:artist/name "bla", :medium/name "6val"}]})))

    (testing "nested complex dependency"
      (is (= (request {}
                      [{[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
                        [{:release/artists
                          [:artist/super-name]}]}])
             {[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
              {:release/artists [{:artist/super-name "SUPER - Horst Jankowski"}]}})))

    (testing "without subquery"
      (is (= (request {}
                      [:artist/artists-before-1600])
             {:artist/artists-before-1600
              [{} {}]}))

      (is (= (request {}
                      [:artist/artist-before-1600])
             {:artist/artist-before-1600
              {}})))

    (testing "ident attributes"
      (is (= (request
               [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                 [:artist/type]}])
             {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
              {:artist/type :artist.type/person}}))
      (is (= (request {}
                      [{[:db/id 637716744120508]
                        [{:artist/type [:db/id]}]}])
             {[:db/id 637716744120508]
              {:artist/type {:db/id 17592186045423}}})))))

(def secure-env
  (-> (pci/register
        [registry])
      (pcd/connect-datomic
        (assoc db-config
          ::pcd/conn conn
          ::pcd/allow-list allow-list
          ::pcd/ident-attributes #{:artist/type}))
      (p.plugin/register (pbip/attribute-errors-plugin))))

(def secure-request
  (p.eql/boundary-interface secure-env))

(deftest test-datomic-secure-parser
  (testing "don't allow access with :db/id"
    (is (= (secure-request
             [{[:db/id 637716744120508]
               [:artist/name]}])
           {[:db/id 637716744120508]
            {:com.wsscode.pathom3.connect.runner/attribute-errors {:artist/name {:com.wsscode.pathom3.error/error-type         :com.wsscode.pathom3.error/node-errors,
                                                                                 :com.wsscode.pathom3.error/node-error-details {1 {:com.wsscode.pathom3.error/error-type :com.wsscode.pathom3.error/attribute-missing}}}}}})))

  (testing "simple read"
    (is (= (secure-request
             {:pathom/entity {:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"}
              :pathom/eql    [:artist/name]})
           {:artist/name "Janis Joplin"})))

  (testing "not found for fields not listed"
    (is (= (secure-request
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/name :artist/gender]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name                                         "Janis Joplin",
             :com.wsscode.pathom3.connect.runner/attribute-errors #:artist{:gender #:com.wsscode.pathom3.error{:error-type :com.wsscode.pathom3.error/attribute-unreachable}}}})))

  (testing "not found for :db/id when its not allowed"
    (is (= (secure-request
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/name :db/id]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"] {:artist/name                                         "Janis Joplin",
                                                                       :com.wsscode.pathom3.connect.runner/attribute-errors {:db/id {:com.wsscode.pathom3.error/error-type         :com.wsscode.pathom3.error/node-errors,
                                                                                                                                     :com.wsscode.pathom3.error/node-error-details {2 {:com.wsscode.pathom3.error/error-type :com.wsscode.pathom3.error/attribute-missing}}}}}})))

  (testing "implicit dependency"
    (is (= (secure-request
             [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
               [:artist/super-name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/super-name "SUPER - Janis Joplin"}})))

  (testing "process-query"
    (is (= (secure-request
             [{:artist/artists-before-1600
               [:artist/super-name
                {:artist/country
                 [:country/name]}]}])
           {:artist/artists-before-1600
            [{:artist/super-name "SUPER - Heinrich Schütz",
              :artist/country    {:country/name "Germany"}}
             {:artist/super-name "SUPER - Choir of King's College, Cambridge",
              :artist/country    {:country/name "United Kingdom"}}]}))

    (is (= (secure-request
             [{:artist/artist-before-1600
               [:artist/super-name
                {:artist/country
                 [:country/name
                  :db/id]}]}])
           {:artist/artist-before-1600 {:artist/super-name "SUPER - Heinrich Schütz",
                                        :artist/country    {:country/name                                        "Germany",
                                                            :com.wsscode.pathom3.connect.runner/attribute-errors {:db/id {:com.wsscode.pathom3.error/error-type         :com.wsscode.pathom3.error/node-errors,
                                                                                                                          :com.wsscode.pathom3.error/node-error-details {5 {:com.wsscode.pathom3.error/error-type :com.wsscode.pathom3.error/attribute-missing}}}}}}}))

    (testing "nested complex dependency"
      (is (= (secure-request
               [{[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
                 [{:release/artists
                   [:artist/super-name]}]}])
             {[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
              {:release/artists [{:artist/super-name "SUPER - Horst Jankowski"}]}})))))
