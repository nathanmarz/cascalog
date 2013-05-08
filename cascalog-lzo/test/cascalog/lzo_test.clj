(ns cascalog.lzo-test
  (:use [cascalog lzo api]
        [midje sweet cascalog])
  (:require [cascalog.io :as io]))

(fact "Test round tripping."
  (io/with-fs-tmp [_ tmp]
    "Set up the job..."
    (?- (hfs-lzo-textline tmp) [["a line of text!"]])
    (with-job-conf lzo-settings
      "The same line of text should come back out."
      (fact?- [["a line of text!"]]
              (hfs-lzo-textline tmp)))))
