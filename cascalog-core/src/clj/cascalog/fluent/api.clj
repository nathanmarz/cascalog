(ns cascalog.fluent.api
  (:use cascalog.fluent.operations
        cascalog.fluent.flow
        cascalog.fluent.tap
        cascalog.fluent.cascading
        cascalog.fluent.def))

;; ## Execution Helpers

(comment
  "TODO: Move to tests."

  (defn square [x] (* x x))
  (defmapop plus-one [x] (inc x))
  (defmapop plus-two [x y z] (inc x))
  (defn add [& xs] (reduce + xs))

  (-> [1 2 3 4 5]
      begin-flow
      (rename* "a")
      ((mapop #'square) "a" "squared")
      (plus-one "a" "b")
      to-memory)

  (-> [[1 2] [2 3] [3 4] [4 5]] ;; or a tap, or anything that can generate.
      begin-flow
      (rename* ["a" "b"])
      (write* (hfs-textline "/tmp/output" :sinkmode :replace))
      (map* #'inc "a" "inc")
      (filter* #'odd? "a")
      (map* #'square "inc" "squared")
      (map* #'dec "squared" "decreased")
      (write* (hfs-textline "/tmp/output4" :sinkmode :replace))
      (to-memory))

  (-> [[1 2] [2 3] [3 4] [4 5]] ;; or a tap, or anything that can generate.
      begin-flow
      (rename* ["a" "b"])
      (write* (hfs-textline "/tmp/output" :sinkmode :replace))
      (map* #'inc "a" "inc")
      (filter* #'odd? "a")
      (map* #'square "inc" "squared")
      (map* #'dec "squared" "decreased")
      (write* (hfs-textline "/tmp/output4" :sinkmode :replace))
      (to-memory))

  (-> [[1 2] [2 3] [3 4] [4 5]] ;; or a tap, or anything that can generate.
      begin-flow
      (rename* ["a" "b"])
      (with-dups ["a" "a" "b"]
        (fn [flow input delta]
          (-> flow (map* #'add input "face"))))
      (write*
       (hfs-textline "/tmp/output4" :sinkmode :replace))
      (graph "/tmp/dotfile.dot")
      (to-memory))

  (let [mk-sink #(hfs-textline % :sinkmode :replace)
        source (-> (begin-flow [[1 2] [2 3] [3 4] [4 5]])
                   (rename* ["a" "b"]))
        a      (-> source
                   (map* #'square "a" "squared")
                   (map* #'dec "b" "decreased")
                   (write* (mk-sink "/tmp/output")))
        b      (-> source
                   (map* #'inc "a" "squared")
                   (map* #'inc "b" "decreased")
                   (write* (mk-sink "/tmp/output2")))]
    (-> (merge* a b)
        (write* (mk-sink "/tmp/outputmerged"))
        (graph "/tmp/dotfile.dot")
        (to-memory)))

  (let [mk-sink #(hfs-textline % :sinkmode :replace)
        source (-> (begin-flow [[1 2] [2 3] [3 4] [4 5]])
                   (rename* ["a" "b"]))
        a      (-> source
                   (map* #'square "a" "squared")
                   (map* #'dec "b" "decreased")
                   (write* (mk-sink "/tmp/output")))
        b      (-> source
                   (map* #'inc "a" "squared")
                   (map* #'inc "b" "decreased")
                   (write* (mk-sink "/tmp/output2")))]
    (all-to-memory a b)))
