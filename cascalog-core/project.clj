(def cc-version (or (System/getenv "CASCALOG_CASCADING_VERSION") "2.0.7"))
(def cascalog-version "2.0.0-SNAPSHOT")

(defproject cascalog/cascalog-core cascalog-version
  :description "Cascalog core libraries."
  :url "http://www.cascalog.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx768m"
             "-server"
             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :repositories {"conjars" "http://conjars.org/repo/"}
  :exclusions [log4j/log4j org.slf4j/slf4j-log4j12]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.macro "0.1.2"]
                 [log4j "1.2.16"]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]
                 [cascading/cascading-hadoop ~cc-version
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core]]
                 [cascading.kryo "0.4.6"]
                 [com.twitter/carbonite "1.3.2"]
                 [com.twitter/maple "0.2.2"]
                 [jackknife "0.1.3"]
                 [hadoop-util "0.3.0"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]]}
             :dev {:resource-paths ["dev"]
                   :plugins [[lein-midje "3.0.0"]]
                   :dependencies
                   [[cascalog/midje-cascalog ~cascalog-version]
                    [org.apache.hadoop/hadoop-core "0.20.2"]]}
             :ci-dev [:dev {:dependencies [[cascalog/midje-cascalog "1.10.1"]
                                           [org.apache.hadoop/hadoop-core "1.1.2"]]}]})
