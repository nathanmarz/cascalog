(def cascalog-version "2.0.1-SNAPSHOT")

(defproject cascalog/cascalog-more-taps cascalog-version
  :description "More taps for Cascalog"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :jar-exclusions [#"\.java$"]
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core ~cascalog-version]
                                       [org.apache.hadoop/hadoop-core "1.1.2"]]}
             :dev {:plugins [[lein-midje "3.0.0"]]
                   :dependencies
                   [[cascalog/midje-cascalog ~cascalog-version]
                    [org.apache.hadoop/hadoop-core "1.1.2"]
                    [hadoop-util "0.3.0"]]}})
