(ns cascalog.fluent.api
  (:use cascalog.fluent.operations
        cascalog.fluent.flow
        cascalog.fluent.tap
        cascalog.fluent.cascading
        cascalog.fluent.def))

;; ## Execution Helpers

(defn exec*
  "Accepts an operation and applies it to the given flow."
  [flow op & args]
  (let [builder (get (meta op) :op-builder map*)]
    (apply builder flow op args)))

(defn ophelper [type builder afn]
  (u/merge-meta afn {::op-builder builder :pred-type type}))

(def filterop* (partial ophelper :filter filter))

(defn filterop [op]
  (fn [flow & more]
    (apply filter* flow op more)))

;; TODO: Alternatively, we could just adorn the function with
;; metadata. Then we could have an apply operation here that would
;; look at the metadata to resolve the appropriate operation.
(defn mapop [op]
  (fn [flow & more]
    (apply map* flow op more)))

(defn mapcatop [op]
  (fn [flow & more]
    (apply mapcat* flow op more)))

(comment
  (defmapop square
    {:prepared true}
    [a b]
    (let []
      (fn [a b])))

  (to-memory
   (-> (begin-flow [[1 1 "a"] [1 2 "a"] [1 3 "a"] [2 4 "b"] [2 5 "b"]])
       (rename* ["k" "v" "letter"])
       (group-by* "k"
                  (par-agg + "v" "sum")
                  (par-agg str "letter" "string")))))
