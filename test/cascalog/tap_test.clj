(ns cascalog.tap-test
  (:use cascalog.tap
        clojure.test
        cascalog.testing)
  (:require [cascalog.io :as io]
            [cascalog.api :as api]
            [cascalog.workflow :as w])
  (:import [cascading.tuple Fields]
           [cascading.tap Tap]
           [cascading.tap.hadoop Hfs Lfs GlobHfs TemplateTap]))

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
     (apply tap-func (w/text-line ["line"] Fields/ALL) "/path/" opts))))

(def hfs-test-source (test-tap-builder hfs-tap tap-source))
(def hfs-test-sink (test-tap-builder hfs-tap tap-sink))
(def lfs-test-source (test-tap-builder lfs-tap tap-source))
(def lfs-test-sink (test-tap-builder lfs-tap tap-sink))

(deftest api-outfields-test
  (are [fields opts]
       (= fields (.getSinkFields (tap-sink (apply api/hfs-textline "path" opts))))
       Fields/ALL []
       (w/fields ["?a"]) [:outfields ["?a"]]
       (w/fields ["?a" "!b"]) [:outfields ["?a" "!b"]]))

(deftest tap-type-test
  (is (instance? TemplateTap (hfs-test-sink :sink-template "%s/")))
  (is (instance? GlobHfs (hfs-test-source :source-pattern "%s/")))
  (is (instance? Hfs (hfs-test-sink :source-pattern "%s/")))
  (is (instance? Hfs (hfs-test-source)))
  (is (instance? Lfs (lfs-test-source))))

(deftest sinkmode-test
  (is (.isKeep (hfs-test-sink)))
  (is (.isKeep (hfs-test-sink :sinkmode :keep)))
  (is (.isUpdate (hfs-test-sink :sinkmode :update)))
  (is (.isReplace (hfs-test-sink :sinkmode :replace))))

(deftest sink-parts-test
  (are [result func opts]
       (= result (.getNumSinkParts (get-scheme (apply func opts))))
       3 hfs-test-sink [:sinkparts 3]
       3 lfs-test-sink [:sinkparts 3]
       0 hfs-test-sink []
       0 lfs-test-sink []))

(deftest tap-pattern-test
  (is (= "%s/" (.getPathTemplate (hfs-test-sink :sink-template "%s/"))))
  (io/with-fs-tmp [_ tmp]
    (let [tuples [[1 2] [2 3] [4 5]]
          temp-tap (api/hfs-seqfile (str tmp "/")
                                    :sink-template "%s/"
                                    :source-pattern "{1,2}/*")]
      temp-tap
      (api/?<- temp-tap [?a ?b] (tuples ?a ?b))
      (test?- [[1 2] [2 3]] temp-tap))))
