(ns cascalog.more-taps-test
  (:use [clojure.test]
        [cascalog api more-taps]
        [midje sweet cascalog])
  (:require [cascalog.cascading.io :as io]
            [hadoop-util.core :as hadoop])
  (:import [org.apache.hadoop.io BytesWritable]))

(background
 (before :facts
         (set-cascading-platform!)))

(deftest delimited-roundtrip-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-textline tmp)   ;; write line
          [["Proin,hendrerit,tincidunt pellentesque"]])
      (fact "Test round trip with hfs-delimited"
        (<- [?a ?b ?c]
            ((hfs-delimited tmp :delimiter ",") ?a ?b ?c)) =>
        (produces [["Proin" "hendrerit" "tincidunt pellentesque"]])))))

(deftest delimited-specify-classes-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-textline tmp)   ;; write line
          [["Proin,false,3"]])
      (fact "Classes with hfs-delimited"
        (<- [?a ?b ?c]
            ((hfs-delimited tmp :delimiter "," :classes [String Boolean Integer]) ?a ?b ?c)) =>
        (produces [["Proin" false 3]])))))

(deftest delimited-compress-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-delimited tmp :delimiter "," :compress? true) ;; write line
          [["Proin" false 3]])
      (fact "Compression with hfs-delimited"
        (<- [?a ?b ?c]
            ((hfs-delimited tmp :delimiter "," :compress? true) ?a ?b ?c)) =>
        (produces [["Proin" "false" "3"]])))))

(deftest delimited-compression-test
  (fact
   (io/with-fs-tmp [_ tmp]
     (?- (hfs-delimited tmp :delimiter "," :compression :disable) ;; write line
         [["Proin" false 3]])
     (fact "Compression with hfs-delimited"
           (<- [?a ?b ?c]
               ((hfs-delimited tmp :delimiter "," :compression :disable) ?a ?b ?c)) =>
               (produces [["Proin" "false" "3"]]))))
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-delimited tmp :delimiter "," :compression :enable) ;; write line
          [["Proin" false 3]])
      (fact "Compression with hfs-delimited"
        (<- [?a ?b ?c]
            ((hfs-delimited tmp :delimiter "," :compression :enable) ?a ?b ?c)) =>
        (produces [["Proin" "false" "3"]])))))

(deftest delimited-inner-quotes-read-delimiter-test
  (fact
   (io/with-fs-tmp [_ tmp]
     (?- (hfs-textline tmp)  ;; write line
         [["Proin,false,3,\"hello, \"\"there\"\"!\""]])
     (fact "Quoted read containing delimiter with hfs-delimited"
       (<- [?a ?b ?c ?d]
           ((hfs-delimited tmp :delimiter "," :quote "\"") ?a ?b ?c ?d)) =>
       (produces [["Proin" "false" "3" "hello, \"there\"!"]])))))

(deftest delimited-inner-quotes-write-delimiter-test
  (fact
   (io/with-fs-tmp [_ tmp]
     (?- (hfs-delimited tmp :delimiter "," :quote "\"")
         [["Proin" "false" "3" "hello, \"there\"!"]])
     (fact "Quoting write containig delimiter with hfs-delimited"
       (<- [?line]
           ((hfs-textline tmp) ?line)) =>
       (produces [["Proin,false,3,\"hello, \"\"there\"\"!\""]])))))

;Note how the last field is quoted by TextDelimited because of the contained delimiter.
;To be completely compliant with rfc4180,
;TextDelimited would have to quote fields containing double quotes as well.
;See section 2.5 and ABNF.
;The following tests check for the current non-quoting behavior.

(deftest delimited-inner-quotes-read-nodelimiter-test
  (fact
   (io/with-fs-tmp [_ tmp]
     (?- (hfs-textline tmp)  ;; write line
         [["Proin,false,3,hello \"\"there\"\"!"]])
     (fact "Quoted read containing only quotes with hfs-delimited"
       (<- [?a ?b ?c ?d]
           ((hfs-delimited tmp :delimiter "," :quote "\"") ?a ?b ?c ?d)) =>
       (produces [["Proin" "false" "3" "hello \"there\"!"]])))))

(deftest delimited-inner-quotes-write-nodelimiter-test
  (fact
   (io/with-fs-tmp [_ tmp]
     (?- (hfs-delimited tmp :delimiter "," :quote "\"")
         [["Proin" "false" "3" "hello \"there\"!"]])
     (fact "Quoting write containing only quotes with hfs-delimited"
       (<- [?line]
           ((hfs-textline tmp) ?line)) =>
       (produces [["Proin,false,3,hello \"\"there\"\"!"]])))))

(deftest wholefile-roundtrip-file-test
  (fact
   (io/with-fs-tmp [fs tmp]
     (let [file-name "file"
           file-path (str tmp "/" file-name)
           binary-data (.getBytes "Proin hendrerit tincidunt pellentesque")
           binary-blob (BytesWritable. binary-data)]
       (with-open [os (.create fs (hadoop/path file-path))]
             (.write os binary-data))
       (fact "Test reading for files with hfs-wholefile"
             (<- [?name ?data]
                 ((hfs-wholefile file-path) ?name ?data)) =>
                 (produces [[file-name binary-blob]]))))))

(deftest wholefile-roundtrip-dir-test
  (fact
   (io/with-fs-tmp [fs tmp]
     (let [file-name-0 "first"
           file-name-1 "second"
           file-path-0 (str tmp "/" file-name-0)
           file-path-1 (str tmp "/" file-name-1)
           binary-data-0 (.getBytes "Proin hendrerit")
           binary-data-1 (.getBytes "tincidunt pellentesque")
           binary-blob-0 (BytesWritable. binary-data-0)
           binary-blob-1 (BytesWritable. binary-data-1)]
       (with-open [os (.create fs (hadoop/path file-path-0))]
         (.write os binary-data-0))
       (with-open [os (.create fs (hadoop/path file-path-1))]
         (.write os binary-data-1))
       (fact "Test reading for directories with hfs-wholefile"
             (<- [?name ?data]
                 ((hfs-wholefile tmp) ?name ?data)) =>
                 (produces [[file-name-0 binary-blob-0]
                            [file-name-1 binary-blob-1]]))))))
