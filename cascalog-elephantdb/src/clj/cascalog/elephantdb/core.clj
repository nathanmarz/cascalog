(ns cascalog.elephantdb.core
  (:use cascalog.api
        [cascalog.elephantdb conf])
  (:require [cascalog.workflow :as w])
  (:import [elephantdb Utils]
           [elephantdb.cascading ElephantDBTap ElephantDBTap$TapMode]
           [org.apache.hadoop.conf Configuration]))

(defn elephant-tap
  [root-path & {:keys [spec sink-fn] :as args}]
  (let [args (convert-args args)
        spec (when spec
               (convert-clj-domain-spec spec))
        source-tap (ElephantDBTap. root-path spec args ElephantDBTap$TapMode/SOURCE)
        sink-tap (ElephantDBTap. root-path spec args ElephantDBTap$TapMode/SINK)]
    (cascalog-tap source-tap
                  (if sink-fn
                    (fn [tuple-src]
                      [sink-tap (sink-fn sink-tap tuple-src)])
                    sink-tap))))
