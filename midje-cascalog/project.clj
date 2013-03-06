(defproject cascalog/midje-cascalog "1.10.1-SNAPSHOT"
  :description "Cascalog functions for Midje."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :dependencies [[midje "1.5-RC1"]]
  :plugins [[lein-midje "3.0-RC1"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0-RC1"]]}
             :dev {:dependencies [[org.apache.hadoop/hadoop-core "1.0.3"]]}
             :provided {:dependencies [[cascalog/cascalog-core "1.10.1-SNAPSHOT"]]}})
