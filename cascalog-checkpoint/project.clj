(defproject cascalog/cascalog-checkpoint "1.10.1-SNAPSHOT"
  :description "Workflow checkpoints for the masses."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :dependencies [[jackknife "0.1.2"]
                 [hadoop-util "0.2.8"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core "1.10.1-SNAPSHOT"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "0.20.2"]]}
             :ci-dev {:dependencies
                      [[org.apache.hadoop/hadoop-core "1.0.3"]]}})
