(ns cascalog.elephantdb.core
  (:use cascalog.api
        [cascalog.elephantdb conf])
  (:require [cascalog.workflow :as w])
  (:import [elephantdb Utils]
           [elephantdb.cascading ElephantDBTap]
           [org.apache.hadoop.conf Configuration]))

(defn elephant-tap
  [root-path & {:keys [spec sink-fn] :as args}]
  (let [args (convert-args args)
        spec (when spec
               (convert-clj-domain-spec spec))
        tap  (ElephantDBTap. root-path spec args)]
    (cascalog-tap tap
                  (if sink-fn
                    (fn [tuple-src]
                      [tap (sink-fn tap tuple-src)])
                    tap))))
