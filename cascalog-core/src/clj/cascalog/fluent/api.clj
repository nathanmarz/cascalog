(ns cascalog.fluent.api
  (:use cascalog.fluent.operations
        cascalog.fluent.operations))

;; ## Execution Helpers

(comment
  "TODO: Move to tests."

  (-> [[1 2] [2 3] [3 4] [4 5]] ;; or a tap, or anything that can generate.
      begin-flow
      (rename* ["a" "b"])
      (write* (:sink (cascalog.api/hfs-textline "/tmp/output" :sinkmode :replace)))
      (map* #'inc "a" "inc")
      (filter* #'odd? "a")
      (map* #'square "inc" "squared")
      (map* #'dec "squared" "decreased")
      (write* (:sink (cascalog.api/hfs-textline "/tmp/output4" :sinkmode :replace)))
      (to-memory)
      #_(run!))

  (let [mk-sink #(:sink (cascalog.api/hfs-textline % :sinkmode :replace))
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

  (let [mk-sink #(:sink (cascalog.api/hfs-textline % :sinkmode :replace))
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
