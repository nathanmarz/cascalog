(ns cascalog.elephantdb.core
  (:use cascalog.api
        [cascalog.elephantdb conf])
  (:require [cascalog.workflow :as w]
            [elephantdb.common.config :as c])
  (:import [elephantdb.index Indexer]
           [cascalog.elephantdb ClojureIndexer]
           [elephantdb Utils]
           [elephantdb.cascading ElephantDBTap]
           [org.apache.hadoop.conf Configuration]))

(defmulti mk-indexer
  "Accepts a var OR a vector of a var and arguments. If this occurs,
  the var will be applied to the other arguments before returning a
  function. For example, given:

  (defn make-adder [x]
      (fn [y] (+ x y)))

  Either of these are valid:

  (mk-indexer [#'make-adder 1])
  (mk-indexer #'inc)

  The supplied function will receive

     [KeyValPersistence, Document]

  as arguments."
  type)

(defmethod mk-indexer Indexer
  [indexer] indexer)

(defmethod mk-indexer :default
  [spec]
  (when spec
    (ClojureIndexer. (w/fn-spec spec))))

(defn elephant-tap
  [root-path & {:keys [spec indexer sink-fn] :as args}]
  (let [args (->  args
                  (merge {:indexer (mk-indexer indexer)})
                  (convert-args))
        spec (when spec
               (c/convert-clj-domain-spec spec))
        tap  (ElephantDBTap. root-path spec args)]
    (cascalog-tap tap
                  (if sink-fn
                    (fn [tuple-src]
                      [tap (sink-fn tap tuple-src)])
                    tap))))
