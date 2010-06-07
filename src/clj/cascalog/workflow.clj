 ;    Copyright 2010 Nathan Marz
 ; 
 ;    This program is free software: you can redistribute it and/or modify
 ;    it under the terms of the GNU General Public License as published by
 ;    the Free Software Foundation, either version 3 of the License, or
 ;    (at your option) any later version.
 ; 
 ;    This program is distributed in the hope that it will be useful,
 ;    but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;    GNU General Public License for more details.
 ; 
 ;    You should have received a copy of the GNU General Public License
 ;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.workflow
  (:refer-clojure :exclude [count first filter mapcat map identity min max])
  (:use [clojure.contrib.seq-utils :only [find-first indexed]])
  (:use cascalog.util)
  (:import [cascading.tuple Tuple TupleEntry Fields]
           [cascading.scheme TextLine SequenceFile]
           [cascading.flow Flow FlowConnector]
           [cascading.cascade Cascades]
           [cascading.operation Identity Insert Debug]
           [cascading.operation.regex RegexGenerator RegexFilter]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup]
           [cascading.pipe.cogroup InnerJoin OuterJoin LeftJoin RightJoin MixedJoin]
           [cascading.scheme Scheme]
           [cascading.tap Hfs Lfs Tap]
           [org.apache.hadoop.io Text]
           [org.apache.hadoop.mapred TextInputFormat TextOutputFormat
                                     OutputCollector JobConf]
           [java.util Properties Map]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
                              ClojureAggregator Util ClojureBuffer]
           [java.io File]
           [java.lang RuntimeException Comparable]))

(defn ns-fn-name-pair [v]
  (let [m (meta v)]
    [(str (:ns m)) (str (:name m))]))

(defn fn-spec [v-or-coll]
  "v-or-coll => var or [var & params]
   Returns an Object array that is used to represent a Clojure function.
   If the argument is a var, the array represents that function.
   If the argument is a coll, the array represents the function returned
   by applying the first element, which should be a var, to the rest of the
   elements."
  (cond
    (var? v-or-coll)
      (into-array Object (ns-fn-name-pair v-or-coll))
    (coll? v-or-coll)
      (into-array Object
        (concat
          (ns-fn-name-pair (clojure.core/first v-or-coll))
          (next v-or-coll)))
    :else
      (throw (IllegalArgumentException. (str v-or-coll)))))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
    obj
    (let [obj (collectify obj)]
      (if (empty? obj) Fields/ALL  ; this is a hack since cascading doesn't support selecting no fields
        (Fields. (into-array String (collectify obj)))
      ))))

(defn fields-array
  [fields-seq]
  (into-array Fields (clojure.core/map fields fields-seq)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn- fields-obj? [obj]
  "Returns true for a Fields instance, a string, or an array of strings."
  (or
    (instance? Fields obj)
    (string? obj)
    (and (sequential? obj) (every? string? obj))))

(defn parse-args
  "
  arr => func-spec in-fields? :fn> func-fields :> out-fields
  
  returns [in-fields func-fields spec out-fields]
  "
  ([arr] (parse-args arr Fields/RESULTS))
  ([arr defaultout]
     (let
       [func-args           (clojure.core/first arr)
        varargs             (rest arr)
        spec                (fn-spec func-args)
        func-var            (if (var? func-args) func-args (clojure.core/first func-args))
                              first-elem (clojure.core/first varargs)
        [in-fields keyargs] (if (or (nil? first-elem)
                                    (keyword? first-elem))
                                  [Fields/ALL (apply hash-map varargs)]
                                  [(fields (clojure.core/first varargs))
                                   (apply hash-map (rest varargs))])
        stateful            (get (meta func-var) :stateful false)
        options             (merge {:fn> (:fields (meta func-var)) :> defaultout} keyargs)
        result              [in-fields (fields (:fn> options)) spec (fields (:> options)) stateful]]

        result )))

(defn pipe
  "Returns a Pipe of the given name, or if one is not supplied with a
   unique random name."
  ([]
   (Pipe. (uuid)))
  ([#^String name]
   (Pipe. name)))

(defn pipe-rename [#^String name]
  (fn [p]
    (Pipe. name p)))

(defn- as-pipes
  [pipe-or-pipes]
  (let [pipes (if (instance? Pipe pipe-or-pipes)
[pipe-or-pipes] pipe-or-pipes)]
  (into-array Pipe pipes)))

;; with a :fn> defined, turns into a function
(defn filter [& args]
  (fn [previous]
    (let [[in-fields func-fields spec out-fields stateful] (parse-args args)]
      (if func-fields
        (Each. previous in-fields
          (ClojureMap. func-fields spec stateful) out-fields)
        (Each. previous in-fields
          (ClojureFilter. spec stateful))))))

(defn mapcat [& args]
  (fn [previous]
    (let [[in-fields func-fields spec out-fields stateful] (parse-args args)]
    (Each. previous in-fields
      (ClojureMapcat. func-fields spec stateful) out-fields))))

(defn map [& args]
  (fn [previous]
    (let [[in-fields func-fields spec out-fields stateful] (parse-args args)]
    (Each. previous in-fields
      (ClojureMap. func-fields spec stateful) out-fields))))

(defn group-by
  ([group-fields]
    (fn [& previous] (GroupBy. (as-pipes previous) (fields group-fields))))
  ([group-fields sort-fields]
    (fn [& previous] (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields))))
  ([group-fields sort-fields reverse-order]
    (fn [& previous] (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields) reverse-order))))

(defn count [#^String count-field]
  (fn [previous]
    (Every. previous (Count. (fields count-field)))))

(defn sum [#^String in-fields #^String sum-fields]
  (fn [previous]
    (Every. previous (fields in-fields) (Sum. (fields sum-fields)))))

(defn min [#^String in-fields #^String min-fields]
  (fn [previous]
    (Every. previous (fields in-fields) (Min. (fields min-fields)))))

(defn max [#^String in-fields #^String max-fields]
  (fn [previous]
    (Every. previous (fields in-fields) (Max. (fields max-fields)))))

(defn first []
  (fn [previous]
    (Every. previous (First.) Fields/RESULTS)))

(defn select [keep-fields]
  (fn [previous]
    (let [ret (Each. previous (fields keep-fields) (Identity.))]
      ret
    )))

(defn identity [& args]
  (fn [previous]
    ;;  + is a hack. TODO: split up parse-args into parse-args and parse-selector-args
    (let [[in-fields func-fields _ out-fields _] (parse-args (cons #'+ args) Fields/RESULTS)]
    (Each. previous in-fields
      (Identity. func-fields) out-fields))))

(defn pipe-name [name]
  (fn [p]
    (Pipe. name p)))

(defn insert [newfields vals]
  (fn [previous]
    (Each. previous (Insert. (fields newfields) (into-array Comparable (collectify vals))) Fields/ALL)))

(defn raw-each
  ([arg1] (fn [p] (Each. p arg1)))
  ([arg1 arg2] (fn [p] (Each. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p] (Each. p arg1 arg2 arg3))))

(defn debug []
  (raw-each (Debug. true)))

(defn raw-every
  ([arg1] (fn [p] (Every. p arg1)))
  ([arg1 arg2] (fn [p] (Every. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p] (Every. p arg1 arg2 arg3))))

(defn aggregate [& args]
  (fn [#^Pipe previous]
    (let [[#^Fields in-fields func-fields specs #^Fields out-fields stateful] (parse-args args Fields/ALL)]
      (Every. previous in-fields
        (ClojureAggregator. func-fields specs stateful) out-fields))))

(defn buffer [& args]
  (fn [#^Pipe previous]
    (let [[#^Fields in-fields func-fields specs #^Fields out-fields stateful] (parse-args args Fields/ALL)]
      (Every. previous in-fields
        (ClojureBuffer. func-fields specs stateful) out-fields))))


;; we shouldn't need a seq for fields (b/c we know how many pipes we have)
(defn co-group
  [fields-seq declared-fields joiner]
  (fn [& pipes-seq]
    (CoGroup.
  	  (pipes-array pipes-seq)
  	  (fields-array fields-seq)
  	  (fields declared-fields)
  	  joiner)))

(defn mixed-joiner [bool-seq]
  (MixedJoin. (boolean-array bool-seq)))

(defn outer-joiner [] (OuterJoin.))

(defn- parse-ophelper-options [options]
  (cond (nil? options)    [nil false]
        (or (sequential? options) (string? options)) [options false]
        (map? options)    [(get options :fields nil) (get options :stateful false)]
        true              (throw (IllegalArgumentException. (str "Invalid options " options)))
        ))

(defn- parse-defop-args [[spec & args]]
  (let [revargs (reverse args)
        code (if (vector? (second revargs)) (reverse (take 2 revargs)) (reverse (take 3 revargs)))
        options (if (= (clojure.core/count code) (clojure.core/count args)) nil (clojure.core/first args)) ]
        [spec options code] ))

;; creates an op that has metadata embedded within it, hack to work around fact that clojure
;; doesn't allow metadata on functions. call (op :meta) to get metadata
;; this is so you can pass operations around and dynamically create flows
(defn- defop-helper [type args]
  (let  [[spec options funcdef]     (parse-defop-args args)
         [declared-fields stateful] (parse-ophelper-options options)
         [hof? fname func-args]     (if (sequential? spec)
                                [true (clojure.core/first spec) (second spec)]
                                [false spec nil])
         runner-name          (symbol (str fname "__"))
         func-form            (if (nil? func-args) `(var ~runner-name) `[(var ~runner-name) ~@func-args])
         args-sym             (gensym "args")
         args-sym-all         (gensym "argsall")
         casclojure-type      (keyword (name type))
         runner-body          (if (nil? func-args)
                                  funcdef
                                  `(~func-args (fn ~@funcdef)))
         assembly-args        (if (nil? func-args)
                                  `[ & ~args-sym]
                                  `[~func-args & ~args-sym])]
  `(do
    (defn ~runner-name {:fields ~declared-fields :stateful ~stateful} ~@runner-body)
    (defn ~fname [ & ~args-sym-all]
      (if (= :meta (clojure.core/first ~args-sym-all))
        {::metadata {:type ~casclojure-type :hof? ~hof?}}
      (let [~assembly-args ~args-sym-all]
        (apply ~type ~func-form ~args-sym)))
      ))))

(defn get-op-metadata
  "Gets metadata of casclojure operation. Returns nil if not an operation. Hack until 
  clojure allows function values to have metadata."
  [op]
  (try (let [ret (op :meta)]
    (if (and (map? ret) (contains? ret ::metadata)) (::metadata ret) nil))
    (catch Exception e nil)))

(defmacro defmapop [& args]
    (defop-helper 'cascalog.workflow/map args))

(defmacro defmapcatop [& args]
    (defop-helper 'cascalog.workflow/mapcat args))

(defmacro deffilterop [& args]
    (defop-helper 'cascalog.workflow/filter args))

(defmacro defaggregateop [& args]
    (defop-helper 'cascalog.workflow/aggregate args))

(defmacro defbufferop [& args]
    (defop-helper 'cascalog.workflow/buffer args))

(defn assemble
  ([x] x)
  ([x form] (apply form (collectify x)))
  ([x form & more] (apply assemble (assemble x form) more)))

(defmacro assembly
  ([args return]
    (assembly args [] return))
  ([args bindings return]
    (let [pipify (fn [forms] (if (or (not (sequential? forms))
                                     (vector? forms))
                              forms
                              (cons 'cascalog.workflow/assemble forms)))
          return (pipify return)
          bindings (vec (clojure.core/map #(%1 %2) (cycle [clojure.core/identity pipify]) bindings))]
      `(fn ~args
          (let ~bindings
            ~return)))))

(defmacro defassembly
  ([name args return]
    (defassembly name args [] return))
  ([name args bindings return]
    `(def ~name (cascalog.workflow/assembly ~args ~bindings ~return))))

(defn join-assembly [fields-seq declared-fields joiner]
  (assembly [& pipes-seq]
    (pipes-seq (co-group fields-seq declared-fields joiner))))

(defn inner-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (InnerJoin.)))

(defn outer-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (OuterJoin.)))

(defn taps-map [pipes taps]
  (Cascades/tapsMap (into-array Pipe pipes) (into-array Tap taps)))

(defn mk-flow [sources sinks assembly]
  (let
    [sources (collectify sources)
     sinks (collectify sinks)
     source-pipes (clojure.core/map #(Pipe. (str "spipe" %2)) sources (iterate inc 0))
     tail-pipes (clojure.core/map #(Pipe. (str "tpipe" %2) %1)
                    (collectify (apply assembly source-pipes)) (iterate inc 0))]
     (.connect (FlowConnector.)
        (taps-map source-pipes sources)
        (taps-map tail-pipes sinks)
        (into-array Pipe tail-pipes))))

(defn text-line
 ([]
  (TextLine.))
 ([field-names]
  (TextLine. (fields field-names) (fields field-names)))
 ([source-fields sink-fields]
  (TextLine. (fields source-fields) (fields sink-fields))))

(defn sequence-file [field-names]
  (SequenceFile. (fields field-names)))

(deffilterop equal [& objs]
  (apply = objs))

(defn compose-straight-assemblies [& all]
  (fn [input]
    (apply assemble input all)))

(defn path
  {:tag String}
  [x]
  (if (string? x) x (.getAbsolutePath #^File x)))

(defn hfs-tap [#^Scheme scheme path-or-file]
  (Hfs. scheme (path path-or-file)))

(defn lfs-tap [#^Scheme scheme path-or-file]
  (Lfs. scheme (path path-or-file)))

(defn write-dot [#^Flow flow #^String path]
  (.writeDOT flow path))

(defn exec [#^Flow flow]
  (.complete flow))