(ns cascalog.fluent.api
  (:use cascalog.fluent.operations
        cascalog.fluent.flow
        cascalog.fluent.tap
        cascalog.fluent.cascading
        cascalog.fluent.def))

;; ## Execution Helpers

(comment
  (to-memory
   (-> (begin-flow [[1 1 "a"] [1 2 "a"] [1 3 "a"] [2 4 "b"] [2 5 "b"]])
       (rename* ["k" "v" "letter"])
       (group-by* "k"
                  (par-agg + "v" "sum")
                  (par-agg str "letter" "string")))))
