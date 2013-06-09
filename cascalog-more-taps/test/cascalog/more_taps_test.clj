(ns cascalog.more-taps-test
  (:use [clojure.test]
        [cascalog api more-taps]
        [midje sweet cascalog])
  (:require [cascalog.io :as io]))

(deftest roundtrip-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-textline tmp)   ;; write line
          [["Proin,hendrerit,tincidunt pellentesque"]])
      (fact "Test round trip"
        (<- [?a ?b ?c]
            ((hfs-delimited tmp :delimiter ",") ?a ?b ?c)) =>
        (produces [["Proin" "hendrerit" "tincidunt pellentesque"]])))))

(deftest specify-classes-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-textline tmp)   ;; write line
          [["Proin,false,3"]])
      (fact "Classes"
        (<- [?a ?b ?c]
            ((hfs-delimited tmp :delimiter "," :classes [String Boolean Integer]) ?a ?b ?c)) =>
        (produces [["Proin" false 3]])))))

(deftest compress-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-delimited tmp :delimiter "," :compress? true)   ;; write line
          [["Proin" false 3]])
      (fact "Compression"
        (<- [?a ?b ?c]
            ((hfs-delimited tmp :delimiter "," :compress? true) ?a ?b ?c)) =>
        (produces [["Proin" "false" "3"]])))))

(deftest inner-quotes-read-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-textline tmp)  ;; write line
          [["Proin,false,3,\"hello \"there\"!\""]])
      (fact
        (<- [?a ?b ?c ?d]
            ((hfs-delimited tmp :delimiter "," :quote "\"") ?a ?b ?c ?d)) =>
        (produces [["Proin" "false" "3" "hello \"there\"!"]])))))

(deftest inner-quotes-write-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-delimited tmp :delimiter "," :quote "\"")
          [["Proin" "false" "3" "\"hello, \"there\"!\""]])
      (fact
        (<- [?line]
            ((hfs-textline tmp) ?line))
        (produces [["Proin,false,3,\"hello, \"there\"!\""]])))))
