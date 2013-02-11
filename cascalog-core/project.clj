(defproject cascalog/cascalog-core "1.10.1-SNAPSHOT"
  :description "Cascalog core libraries."
  :url "http://www.cascalog.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx768m"
             "-server"
             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :javac-options {:target "1.6" :source "1.6"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :repositories {"conjars" "http://conjars.org/repo/"}
  :plugins [[lein-midje "3.0-alpha1"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.macro "0.1.1"]
                 [ch.qos.logback/logback-classic "1.0.6"]
                 [org.slf4j/log4j-over-slf4j "1.6.6"]
                 [cascading/cascading-hadoop "2.0.3"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core
                               org.slf4j/slf4j-log4j12]]
                 [cascading.kryo "0.4.5"]
                 [com.twitter/carbonite "1.3.1"]
                 [hadoop-util "0.2.8"]
                 [com.twitter/maple "0.2.2"]
                 [jackknife "0.1.2"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0-RC1"]]}
             :dev {:resource-paths ["dev"]
                   :dependencies
                   [[org.apache.hadoop/hadoop-core "0.20.2-dev" :exclusions [org.slf4j/slf4j-log4j12]]
                    [midje "1.5-alpha10"]
                    [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]}})
