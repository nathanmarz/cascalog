(defproject cascalog/cascalog-lzo "1.10.1-SNAPSHOT"
  :description "Lzo compression taps for Cascalog."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"bird" "https://raw.github.com/kevinweil/elephant-bird/master/repo"
                 "conjars.org" "http://conjars.org/repo"}
  :dependencies [[cascalog "1.10.1-SNAPSHOT"]
                 [cascalog/elephant-bird "2.2.3-SNAPSHOT"]
                 [hadoop-lzo "0.4.15"]]
  :exclusions [yamlbeans
               com.hadoop/hadoop-lzo
               org.apache.thrift/libthrift
               org.apache.pig/pig
               org.apache.pig/piggybank
               org.apache.hive/hive-serde
               org.apache.hive/hive-exec
               org.apache.hadoop/hadoop-core
               org.apache.mahout/mahout-core
               org.apache.hcatalog/hcatalog]
  :plugins [[lein-midje "3.0-alpha4"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0-RC1"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "1.0.3"]
                    [midje "1.5-alpha10"]
                    [midje-cascalog "0.4.0"
                     :exclusions [org.clojure/clojure]]]}})
