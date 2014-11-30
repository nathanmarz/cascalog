(ns cascalog.cascading.conf-test
  (:use midje.sweet
        cascalog.cascading.conf)
  (:require [clojure.string :as s]
            [cascalog.api :refer (??<- set-cascading-platform!)])
  (:import [cascalog.hadoop DefaultComparator]
           [cascading.flow.planner PlannerException]))

(background
 (before :facts
         (set-cascading-platform!)))

(def comma
  (partial s/join ","))

(def defaults
  (comma default-serializations))

(with-job-conf {"key" "val"}
  (fact "The first binding level should set the JobConf equal to the
      supplied conf map."
    *JOB-CONF* => {"key" "val"}))

(with-job-conf {"key" ["val1" "val2"]
                "other-key" "other-val"}
  (fact "Vectors of strings will be joined w/ commas. This binding
      level knocks out the previous, as the keys are identical."
    *JOB-CONF* => {"key" "val1,val2"
                        "other-key" "other-val"})

  (with-job-conf {"key" ["val3"]}
    (fact "other-key from above should be preserved."
      *JOB-CONF* => {"key" "val3"
                          "other-key" "other-val"})))

(with-job-conf {"io.serializations" "java.lang.String"}
  *JOB-CONF* => {"io.serializations" "java.lang.String"}
  (fact
    "Calling project-merge on the JobConf will prepend default
      serializations onto the supplied list of serializations."
    (project-merge *JOB-CONF*) => {"io.serializations"
                                   (comma [defaults "java.lang.String"])})
  (with-serializations [String]
    (fact
      "You can specify serialiations using the `with-serializations`
        form. This works w/ class objects or strings. Without
        project-merge, the *JOB-CONF* variable is unaffected. (Note that
        classes are resolved properly.)"
      *JOB-CONF* => {"io.serializations" "java.lang.String"})

    (fact "Again, project-merging with w/ Class objects, vs Strings."
      (project-merge *JOB-CONF*) => {"io.serializations"
                                     (comma [defaults "java.lang.String"])})))

(with-serializations [String]
  (with-job-conf {"io.serializations" "java.lang.String,SomeSerialization"}
    (fact "with-serializations nests properly with with-job-conf."
      (project-merge *JOB-CONF*) => {"io.serializations"
                                     (comma [defaults
                                             "java.lang.String"
                                             "SomeSerialization"])})))

(facts "Tests of various aspects of Kryo serialization."
  (with-job-conf
    {"com.twitter.chill.config.reflectinginstantiator.registrations" "java.util.DoesntExist,someSerializer"
     "com.twitter.chill.config.reflectinginstantiator.skipmissing" true
     "com.twitter.chill.config.reflectinginstantiator.registrationrequired" false}
    (let [cal-tuple [[(java.util.GregorianCalendar.)]]]
      (??<- [?a] (cal-tuple ?a)) => cal-tuple))

  (with-job-conf
    {"com.twitter.chill.config.reflectinginstantiator.registrationrequired" true}
    (let [cal-tuple [[(java.util.GregorianCalendar.)]]]
      (fact
        "Attempting to serialize an unregistered object when
          accept.all is set to false should throw a flow exception."
        (??<- [?a] (cal-tuple ?a))) => (throws PlannerException))))

(tabular
 (fact "Test of various comparators."
   (let [comp (DefaultComparator.)]
     (.compare comp ?left ?right) => ?expected))
 ?left     ?right       ?expected
 0         0            0
 1         1            0
 1         1M           0
 1M        1            0
 2M        1            1
 (Long. 4) (Integer. 3) 1
 (Long. 3) (Integer. 4) -1
 1         2M           -1)

(fact "Conf-merging test."
  (let [m1 {"key" "foo"
            "key2" ["bar" "baz"]}
        m2 {"key" ["cake" "salad"]}]
    (conf-merge m1)    => {"key" "foo", "key2" "bar,baz"}
    (conf-merge m1 m2) => {"key" "cake,salad", "key2" "bar,baz"}))

(fact "Stringify test."
  (stringify-keys
   {:key "val" "key2" "val2"}) => {"key" "val" "key2" "val2"})

(future-fact
 "Test that stringify-keys can handle clashes between,
  say, \"key\" and :key.")
