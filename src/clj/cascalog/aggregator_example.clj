; (ns cascading.clojure.aggregator-example
;   (:require (cascading.clojure [api :as c])))
; 
; (defn add-counts-start []
;   0)
; 
; (defn add-counts-aggregate [mem _ v]
;   (+ mem v))
; 
; (defn add-counts-complete [mem]
;   [mem])
; 
; (defn parse-pair [line]
;   (let [[word num-str] (re-seq #"\w+" line)]
;     [word (Integer/valueOf num-str)]))
; 
; (def summer
;   (c/assemble (c/pipe "summer")
;     (c/map "line" [["word" "subcount"] #'parse-pair])
;     (c/group-by "word")
;     (c/aggregate ["word" "subcount"] "count"
;       #'add-counts-start #'add-counts-aggregate #'add-counts-complete)
;     (c/select ["word" "count"])))
; 
; (defn run-example
;   [jar-path dot-path in-dir-path out-dir-path]
;   (let [source-scheme (c/text-line-scheme ["line"])
;         sink-scheme   (c/text-line-scheme ["word" "count"])
;         source        (c/hfs-tap source-scheme in-dir-path)
;         sink          (c/hfs-tap sink-scheme out-dir-path)
;         flow          (c/flow
;                         jar-path
;                         {}
;                         {"summer" source}
;                         sink
;                         summer)]
;     (c/write-dot flow dot-path)
;     (c/complete flow)))
