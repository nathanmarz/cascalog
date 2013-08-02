(def cascalog-version "2.0.0-SNAPSHOT")

(defproject cascalog/cascalog-checkpoint cascalog-version
  :description "Workflow checkpoints for the masses."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :dependencies [[jackknife "0.1.5"]
                 [hadoop-util "0.3.0"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core ~cascalog-version]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "1.1.2"]]}})
