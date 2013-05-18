(ns cascalog.fluent.api
  (:use cascalog.fluent.operations
        cascalog.fluent.flow
        cascalog.fluent.tap
        cascalog.fluent.cascading
        cascalog.fluent.def)
  (:require [cascalog.fluent.fn :as serfn]))

;; ## Execution Helpers

(comment
  (defmapfn square
    [x] (* x x))

  (to-memory
   (-> (generator [[1 1 "a"] [1 2 "a"] [1 3 "a"] [2 4 "b"] [2 5 "b"]])
       (rename* ["k" "v" "letter"])
       (exec* (mapop* inc) "k" "inck")))

  (to-memory
   (-> (begin-flow [[1 1 "a"] [1 2 "a"] [1 3 "a"] [2 4 "b"] [2 5 "b"]])
       (rename* ["k" "v" "letter"])
       (group-by* "k"
                  (parallel-agg + "v" "sum")
                  (parallel-agg str "letter" "string")))))
