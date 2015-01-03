(ns cascalog.in-memory.testing
  (:require [cascalog.api :refer :all]
            [cascalog.logic.testing :refer (ITestable)]
            [cascalog.in-memory.tuple :refer (map-select-values)]
            [jackknife.seq :refer (unweave)])
  (:import [cascalog.in_memory.platform InMemoryPlatform]))

(extend-protocol ITestable
  InMemoryPlatform
  (process?- [_ [ll :as bindings]]
    (let [bindings (if (keyword? ll)
                     (rest bindings)
                     bindings)
          [specs rules] (unweave bindings)
          out-tuples (map
                        #(let [results (atom [])
                               fields (get-out-fields %)]
                           (?- results %)
                           (map-select-values fields @results))
                        rules)]
      [specs out-tuples])))
