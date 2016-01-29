(ns cascalog.cascading.tap-test
  (:use [midje sweet cascalog]
        clojure.test
        cascalog.logic.testing
        cascalog.cascading.testing
        cascalog.api)
  (:require [cascalog.cascading.io :as io]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.util :refer (fields)])
  (:import [cascading.scheme.hadoop TextLine$Compress]
           [cascading.tuple Fields]
           [cascading.tap Tap]
           [cascading.tap.hadoop Hfs Lfs GlobHfs TemplateTap]))

(background
 (before :facts
         (set-cascading-platform!)))

(defn tap-source [tap]
  (if (map? tap)
    (recur (:source tap))
    tap))

(defn tap-sink [tap]
  (if (map? tap)
    (recur (:sink tap))
    tap))

(defn get-scheme [^Tap tap]
  (.getScheme tap))

(defn test-tap-builder [tap-func extracter]
  (fn [& opts]
    (extracter
     (apply tap-func
            (tap/text-line ["line"] Fields/ALL)
            "/path/"
            opts))))

(def hfs-test-source (test-tap-builder tap/hfs-tap tap-source))
(def hfs-test-sink (test-tap-builder tap/hfs-tap tap-sink))
(def lfs-test-source (test-tap-builder tap/lfs-tap tap-source))
(def lfs-test-sink (test-tap-builder tap/lfs-tap tap-sink))

(deftest api-outfields-test
  (tabular
    (facts "api outfields testing."
      (-> (apply tap/hfs-textline "path" ?opts)
          (tap-sink)
          (.getSinkFields)) => ?fields)
    ?fields                ?opts
    (fields Fields/ALL)  []
    (fields ["?a"])      [:outfields ["?a"]]
    (fields ["?a" "!b"]) [:outfields ["?a" "!b"]]))

(deftest api-compression-test
  (tabular
   (facts "api compression testing"
     (-> (apply tap/hfs-textline "path" ?opts)
         (tap-sink)
         (.getScheme)
         (.getSinkCompression)
         (.ordinal)) => ?compression-value)
   ?compression-value                     ?opts
   (.ordinal TextLine$Compress/DEFAULT)   []
   (.ordinal TextLine$Compress/DEFAULT)   [:compression :default]
   (.ordinal TextLine$Compress/ENABLE)    [:compression :enable]
   (.ordinal TextLine$Compress/DISABLE)   [:compression :disable]))

(deftest taps-type-test
  (tabular
    (fact "Type testing on the various taps."
      ?tap => (fn [x] (instance? ?type x)))
    ?type        ?tap
    TemplateTap (hfs-test-sink :sink-template "%s/")
    GlobHfs     (hfs-test-source :source-pattern "%s/")
    Hfs         (hfs-test-sink :source-pattern "%s/")
    Hfs         (hfs-test-source)
    Lfs         (lfs-test-source)))

(deftest sinkmode-test
  (fact "SinkMode testing."
    (hfs-test-sink) => (memfn isKeep)
    (hfs-test-sink :sinkmode :keep) => (memfn isKeep)
    (hfs-test-sink :sinkmode :update) => (memfn isUpdate)
    (hfs-test-sink :sinkmode :replace) => (memfn isReplace)))

(deftest sinkparts-test
  (tabular
    (fact ":sinkparts keyword should tune sink settings."
      (-> (apply ?func ?opts)
          (get-scheme)
          (.getNumSinkParts)) => ?result)
    ?result ?func         ?opts
    3       hfs-test-sink [:sinkparts 3]
    3       lfs-test-sink [:sinkparts 3]
    0       hfs-test-sink []
    0       lfs-test-sink []))

(deftest sink-template-test
  (fact
    ":sink-template option should set path template on cascading tap."
    (.getPathTemplate (hfs-test-sink :sink-template "%s/")) => "%s/")
  (fact 
   ":open-threshold should set open taps threshold"
   (.getOpenTapsThreshold 
    (hfs-test-sink :sink-template "%s/" :open-threshold 10)) => 10))

(deftest template-tap-test
  (fact
    "Test executing tuples into a template tap and sourcing them back
    out with a source pattern."
    (io/with-log-level :fatal
      (io/with-fs-tmp [_ tmp]
        (let [tuples [[1 2] [2 3] [4 5]]
              temp-tap (tap/hfs-seqfile (str tmp "/")
                                        :sink-template "%s/"
                                        :source-pattern "{1,2}/*")]
          temp-tap
          (?<- temp-tap [?a ?b] (tuples ?a ?b))
          temp-tap => (produces [[1 2] [2 3]]))))))

(deftest glob-test
  (tabular
   (fact "GlobHfs testing with various globs."
     (let [glob-tap (hfs-tap (tap/text-line) "src" :source-pattern ?pattern)
           child-taps (iterator-seq (.getChildTaps (tap-source glob-tap)))]
       (map #(-> (.getPath %) (.getName)) child-taps)) => ?files)
       ?pattern   ?files
       "/*"        ["clj" "java"]
       "/**"       ["clj" "java"]
       "/*/"       ["clj" "java"]
       "/../src/*" ["clj" "java"]
       "*/*"       ["clj" "java"]
       "/clj"      ["clj"]
       "/*/*"      ["cascalog" "cascading" "cascalog" "jcascalog"]
       "/."        ["src"]
       "*"         ["src"]
       "/"         ["src"]))
