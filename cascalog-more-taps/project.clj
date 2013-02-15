(defproject cascalog-more-taps "1.10.1-SNAPSHOT"
  :description "More taps for Cascalog"
  :min-lein-version "2.0.0"
  :license {:name "MIT License"
            :url "http://www.opensource.org/licenses/MIT"}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.4.0"]
                                  [cascalog "1.10.1-SNAPSHOT"]
                                  [cascading/cascading-hadoop "2.0.4"]
                                  [org.apache.hadoop/hadoop-core "0.20.2-dev"]]}}
  :repositories {"conjars.org" "http://conjars.org/repo"})