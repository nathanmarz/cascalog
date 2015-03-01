(ns cascalog.lzo.impl
  (:require [cascalog.cascading.util :as util])
  (:import [com.twitter.elephantbird.cascading2.scheme
            LzoTextLine LzoTextDelimited LzoThriftScheme
            LzoProtobufScheme]))

(defn text-line
  ([] (LzoTextLine.))
  ([field-names]
     (text-line field-names field-names))
  ([source-fields sink-fields]
     (LzoTextLine. (util/fields source-fields)
                   (util/fields sink-fields))))

(defn delimited [field-names klasses]
  (let [klasses (when klasses (into-array klasses))]
    (-> (util/fields field-names)
        (LzoTextDelimited. "\t"))))

(defn thrift-b64-line [klass]
  (LzoThriftScheme. klass))

(defn proto-b64-line [klass]
  (LzoProtobufScheme. klass))
