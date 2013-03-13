(defproject cascalog/cascalog-elephantdb "1.10.1-SNAPSHOT"
  :description "ElephantDB Integration for Cascalog."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-server" "-Xmx1024m"]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [elephantdb/elephantdb-cascading "0.4.0-RC1"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core "1.10.1-SNAPSHOT"]
                                       [org.apache.hadoop/hadoop-core "1.0.3"]]}
             :dev {:dependencies
                   [[elephantdb/elephantdb-bdb "0.4.0-RC1"]
                    [midje "1.5.0"]
                    [midje-cascalog "1.10.1-SNAPSHOT"]]
                   :plugins [[lein-midje "3.0.0"]]}})
