(defproject cascalog/cascalog-checkpoint "1.10.1-SNAPSHOT"
  :description "Workflow checkpoints for the masses."
  :dependencies [[cascalog/cascalog-core "1.10.1-SNAPSHOT"]
                 [jackknife "0.1.2"]
                 [hadoop-util "0.2.8"]]
  :plugins [[lein-midje "3.0-alpha4"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.2.1"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0-RC1"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "1.0.3"]
                    [midje "1.5-alpha10"]
                    [midje-cascalog "0.4.0"
                     :exclusions [org.clojure/clojure]]]}})
