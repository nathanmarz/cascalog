(ns cascalog.elephantdb.keyval
  (:use cascalog.api)
  (:require [cascalog.workflow :as w]
            [cascalog.elephantdb.core :as core]
            [elephantdb.common.config :as c])
  (:import [elephantdb.index Indexer]
           [elephantdb.cascading KeyValGateway]
           [cascalog.elephantdb KeyValIndexer]
           [cascalog.ops IdentityBuffer]
           [org.apache.hadoop.conf Configuration]
           [elephantdb Utils]
           [elephantdb.partition ShardingScheme]
           [org.apache.hadoop.io BytesWritable]))

(defn- test-array
  [t]
  (let [check (type (t []))]
    (fn [arg] (instance? check arg))))

(def ^{:private true} byte-array?
  (test-array byte-array))

(defmapop [shard [^ShardingScheme scheme shard-count]]
  "Returns the shard to which the supplied shard-key should be
  routed."
  [^bytes shard-key]
  {:pre [(byte-array? shard-key)]}
  (.shardIndex scheme shard-key shard-count))

(defmapop mk-sortable-key [^bytes shard-key]
  {:pre [(byte-array? shard-key)]}
  (BytesWritable. shard-key))

(defn elephant<-
  [elephant-tap kv-src]
  (let [spec        (.getSpec elephant-tap)
        scheme      (.getShardScheme spec)
        shard-count (.getNumShards spec)]
    (<- [!shard !key !value]
        (kv-src !keyraw !valueraw)
        (shard [scheme shard-count] !keyraw :> !shard)
        (mk-sortable-key !keyraw :> !sort-key)
        (:sort !sort-key)
        ((IdentityBuffer.) !keyraw !valueraw :> !key !value))))

(defmulti kv-indexer
  "Accepts a var OR a vector of a var and arguments. If this occurs,
  the var will be applied to the other arguments before returning a
  function. For example, given:

  (defn make-adder [x]
      (fn [y] (+ x y)))

  Either of these are valid:

  (kv-indexer [#'make-adder 1])
  (kv-indexer #'inc)

  The supplied function will receive

     [KeyValPersistence, key, value]

  as arguments."
  type)

(defmethod kv-indexer Indexer
  [indexer] indexer)

(defmethod kv-indexer :default
  [spec]
  (when spec
    (KeyValIndexer. (w/fn-spec spec))))

(defn keyval-tap
  "Returns a tap that can be used to source and sink key-value pairs
  to ElephantDB."
  [root-path & {:keys [indexer] :as args}]
  (let [args (merge {:gateway (KeyValGateway.)
                     :source-fields ["key" "value"]}
                    args
                    {:sink-fn elephant<-
                     :indexer (kv-indexer indexer)})]
    (apply core/elephant-tap
           root-path
           (apply concat args))))

(defn reshard!
  "Accepts two target paths and a new shard count and re-shards the
  domain at source-dir into target-dir. (To re-shard a domain into
  itself, pass the same path in for source and target.)"
  [source-dir target-dir shard-count]
  (let [fs (Utils/getFS source-dir (Configuration.))
        spec (c/read-domain-spec fs source-dir)
        new-spec (assoc spec :num-shards shard-count)]
    (?- (keyval-tap target-dir :spec new-spec)
        (keyval-tap source-dir))))
