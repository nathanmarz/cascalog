(ns cascalog.cascading.io-test
  (:use cascalog.cascading.io
        midje.sweet
        clojure.test)
  (:require [cascalog.api :as api]))

(deftest configurable-with-fs-tmp
  (is (.startsWith (with-fs-tmp [_ foo] foo)
                   "/tmp/cascalog_reserved/"))
  (is (.startsWith (api/with-job-conf
                     {tmp-dir-property
                      ;; deliberately using lein's build directory
                      "target/bar"}
                     (with-fs-tmp [_ foo] foo))
                   "target/bar/cascalog_reserved/")))
