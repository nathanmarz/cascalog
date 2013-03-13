(defproject cascalog/cascalog-more-taps "1.10.1-SNAPSHOT"
  :description "More taps for Cascalog"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :plugins [[lein-midje "3.0.0"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core "1.10.1-SNAPSHOT"]]}
             :dev {:dependencies
                   [[org.apache.hadoop/hadoop-core "1.0.3"]
                    [cascalog/midje-cascalog "1.10.1-SNAPSHOT" :exclusions [org.clojure/clojure]]]}})
