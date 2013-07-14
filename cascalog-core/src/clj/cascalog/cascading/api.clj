(ns cascalog.cascading.api
  (:use cascalog.logic.def
        cascalog.cascading.operations
        cascalog.cascading.flow
        cascalog.cascading.types
        cascalog.cascading.tap
        cascalog.cascading.util)
  (:require [cascalog.logic.fn :as serfn]))

;; ## Execution Helpers

(comment
  (defmapfn square
    [x] (* x x))

  (to-memory
   (-> (begin-flow [[1 1 "a"] [1 2 "a"] [1 3 "a"] [2 4 "b"] [2 5 "b"]])
       (rename* ["k" "v" "letter"])
       (group-by* "k"
                  (parallel-agg + "v" "sum")
                  (parallel-agg str "letter" "string")))))
