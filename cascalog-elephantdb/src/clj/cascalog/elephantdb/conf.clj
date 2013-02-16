(ns cascalog.elephantdb.conf
  (:require [cascalog.workflow :as w])
  (:import [elephantdb.cascading ElephantDBTap$Args]
           [java.util ArrayList]))

(defn convert-args
  [{:keys [incremental tmp-dirs indexer source-fields
           timeout-ms version gateway]
    :or {incremental true}}]
  (let [mk-list (fn [xs] (when xs (ArrayList. xs)))
        ret      (ElephantDBTap$Args.)]
    (set! (.incremental ret) incremental)
    (when source-fields
      (set! (.sourceFields ret) (w/fields source-fields)))
    (set! (.tmpDirs ret) (mk-list tmp-dirs))
    (when gateway
      (set! (.gateway ret) gateway))
    (when indexer
      (set! (.indexer ret) indexer))
    (when timeout-ms
      (set! (.timeoutMs ret) timeout-ms))
    (set! (.version ret) version)
    ret))
