(ns cascalog.in-memory.testing
  (:require [cascalog.api :refer :all]
            [cascalog.logic.testing :refer (ITestable)]
            [jackknife.seq :refer (unweave)])
  (:import [cascalog.in_memory.platform InMemoryPlatform]))

(extend-protocol ITestable
  InMemoryPlatform
  (process?- [_ [ll :as bindings]]
    (let [bindings (if (keyword? ll)
                     (rest bindings)
                     bindings)
          [specs rules] (unweave bindings)
          out-tuples (apply ??- rules)]
      [specs out-tuples])))
