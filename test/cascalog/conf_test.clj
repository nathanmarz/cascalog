(ns cascalog.conf-test
  (:use clojure.test
        [cascalog api testing])
  (:require [clojure.string :as s]
            [cascalog.conf :as conf]
            [cascalog.util :as u])
  (:import [cascalog.hadoop DefaultComparator]))

(def comma
  (partial s/join ","))

(def defaults
  (comma u/default-serializations))

(deftest test-jobconf-bindings
  (with-job-conf {"key" "val"}
    (is (= conf/*JOB-CONF*
           {"key" "val"})))

  (with-job-conf {"key" ["val1" "val2"]}
    (is (= conf/*JOB-CONF*
           {"key" "val1,val2"}))
    (with-job-conf {"key" ["val3"]}
      (is (= conf/*JOB-CONF*
             {"key" "val3"}))))
  
  (with-job-conf {"io.serializations" "java.lang.String"}
    (is (= conf/*JOB-CONF*
           {"io.serializations" "java.lang.String"}))
    (is (= (u/project-merge conf/*JOB-CONF*)
           {"io.serializations" (comma [defaults "java.lang.String"])})))

  (with-serializations [String]
    (is (= conf/*JOB-CONF*
           {"io.serializations" "java.lang.String"}))
    (is (= (u/project-merge conf/*JOB-CONF*)
           {"io.serializations" (comma [defaults "java.lang.String"])})))

  (with-serializations [String]
    (with-job-conf {"io.serializations" "java.lang.String,SomeSerialization"}
      (is (= (u/project-merge conf/*JOB-CONF*)
             {"io.serializations"
              (comma [defaults "java.lang.String" "SomeSerialization"])})))))

(deftest kryo-serialization-test
  (with-job-conf
    {"cascading.kryo.serializations" "java.util.DoesntExist"
     "cascading.kryo.skip.missing" true
     "cascading.kryo.accept.all" true}
    (let [cal-tuple [[(java.util.GregorianCalendar.)]]]
      (test?<- cal-tuple [?a] (cal-tuple ?a))))
  (with-job-conf
    {"cascading.kryo.accept.all" false}
    (let [cal-tuple [[(java.util.GregorianCalendar.)]]]
      (is (thrown? RuntimeException
                   (??<- [?a] (cal-tuple ?a)))))))

(deftest default-comparator-test
  "Tests of the default comparator and hasher we use for all cascading
   operations."
  (let [comp (DefaultComparator.)]
    (testing "Overridden comparisons."
      (are [x y] (= 0 (.compare comp x y))
           0 0
           1 1
           1 1M
           1M 1)
      (are [x y] (= 1 (.compare comp x y))
           2M 1
           (Long. 4) (Integer. 3))
      (are [x y] (= -1 (.compare comp x y))
           1 2M
           (Long. 3) (Integer. 4)))
    (testing "Hashcode equality."
      (are [x y] (= (.hashCode comp x)
                    (.hashCode comp y))
           0 0
           1 1
           {1M "hi!"} {(Long. 1) "hi!"}
           {1 2, 3 4} {3 4, 1 2}
           (Long. 1) (Integer. 1)
           (Long. 1) (BigDecimal. 1)))))
