(ns cascalog.elephantdb.conf
  (:require [cascalog.workflow :as w])
  (:import [elephantdb DomainSpec]
           [elephantdb.cascading ElephantDBTap$Args]
           [java.util ArrayList]))

(defn convert-java-domain-spec [^DomainSpec spec]
  {:coordinator  (.getCoordinator spec)
   :shard-scheme (.getShardScheme spec)
   :num-shards   (.getNumShards spec)})

(defn convert-clj-domain-spec
  [{:keys [coordinator shard-scheme num-shards]}]
  {:pre [(and coordinator shard-scheme num-shards)]}
  (DomainSpec. coordinator shard-scheme num-shards))

(defn read-domain-spec
  "A domain spec is stored with shards in the VersionedStore. Look to
  s3 for an example here."
  [fs path]
  (when-let [spec (DomainSpec/readFromFileSystem fs path)]
    (convert-java-domain-spec spec)))

(defn convert-args
  [{:keys [tmp-dirs source-fields
           timeout-ms version gateway]}]
  (let [mk-list (fn [xs] (when xs (ArrayList. xs)))
        ret      (ElephantDBTap$Args.)]
    (when source-fields
      (set! (.sourceFields ret) (w/fields source-fields)))
    (set! (.tmpDirs ret) (mk-list tmp-dirs))
    (when gateway
      (set! (.gateway ret) gateway))
    (when timeout-ms
      (set! (.timeoutMs ret) timeout-ms))
    (set! (.version ret) version)
    ret))
