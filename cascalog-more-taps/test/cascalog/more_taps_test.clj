(ns cascalog.more-taps-test
  (:use [cascalog api more-taps]
        [midje sweet cascalog])
  (:require [cascalog.io :as io]))

(fact
  (io/with-fs-tmp [_ tmp]
    (?- (hfs-textline tmp)   ;; write line
        [["Proin,hendrerit,tincidunt pellentesque"]])
    (fact "Test round trip"
      (<- [?a ?b ?c]
          ((hfs-delimited tmp :delimiter ",") ?a ?b ?c)) =>
      (produces [["Proin" "hendrerit" "tincidunt pellentesque"]]))))

(fact
  (io/with-fs-tmp [_ tmp]
    (?- (hfs-textline tmp)   ;; write line
        [["Proin,false,3"]])
    (fact "Classes"
      (<- [?a ?b ?c]
          ((hfs-delimited tmp :delimiter "," :classes [String Boolean Integer]) ?a ?b ?c)) =>
      (produces [["Proin" false 3]]))))

(fact
  (io/with-fs-tmp [_ tmp]
    (?- (hfs-delimited tmp :delimiter "," :compress true)   ;; write line
        [["Proin" false 3]])
    (fact "Compression"
      (<- [?a ?b ?c]
          ((hfs-delimited tmp :delimiter "," :compress true) ?a ?b ?c)) =>
      (produces [["Proin" "false" "3"]]))))
