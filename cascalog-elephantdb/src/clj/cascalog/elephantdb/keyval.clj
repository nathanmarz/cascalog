(ns cascalog.elephantdb.keyval
  (:use cascalog.api
        cascalog.elephantdb.conf)
  (:require [cascalog.workflow :as w]
            [cascalog.elephantdb.core :as core])
  (:import [cascalog.ops IdentityBuffer]
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

(defn keyval-tap
  "Returns a tap that can be used to source and sink key-value pairs
  to ElephantDB."
  [root-path & {:keys [] :as args}]
  (let [args (merge {:source-fields ["key" "value"]}
                    args
                    {:sink-fn elephant<-})]
    (apply core/elephant-tap
           root-path
           (apply concat args))))

(defn reshard!
  "Accepts two target paths and a new shard count and re-shards the
  domain at source-dir into target-dir. (To re-shard a domain into
  itself, pass the same path in for source and target.)"
  [source-dir target-dir shard-count]
  (let [fs (Utils/getFS source-dir (Configuration.))
        spec (read-domain-spec fs source-dir)
        new-spec (assoc spec :num-shards shard-count)]
    (?- (keyval-tap target-dir :spec new-spec)
        (keyval-tap source-dir))))
