(ns cascalog.elephantdb.example
  (:import [elephantdb.persistence KeyValPersistence]
           [elephantdb.document KeyValDocument]))

(defn str-indexer
  [^KeyValPersistence lp ^KeyValDocument doc]
  (let [key (.key doc)
        val (.value doc)
        new-val (str (.get lp key) val)]
    (->> (KeyValDocument. key new-val)
         (.index lp))))

(defn str-kv-indexer
  [^KeyValPersistence lp key val]
  (let [old-val (.get lp key)]
    (.put lp key (str old-val val))))
