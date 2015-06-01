(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def HADOOP-VERSION (-> ROOT-DIR (str "/../HADOOP-VERSION") slurp))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))
(def CC-VERSION (or (System/getenv "CASCALOG_CASCADING_VERSION") "3.0.0-wip-116"))

(defproject cascalog/cascalog-core-tez VERSION
  :description "Cascalog core libraries."
  :url "http://www.cascalog.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx768m"
             "-server"
             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :javac-options ["-target" "1.7" "-source" "1.7"]
  :aot [cascalog.cascading.tap
        cascalog.cascading.types
        cascalog.cascading.operations        
        cascalog.cascading.conf
        cascalog.cascading.def
        cascalog.cascading.flow
        cascalog.cascading.io
        cascalog.cascading.platform
        cascalog.cascading.stats        
        cascalog.cascading.util        
        cascalog.logic.def     
        cascalog.logic.predicate
        cascalog.logic.parse
        cascalog.logic.platform
        cascalog.logic.algebra
        cascalog.logic.fn
        cascalog.logic.ops-impl
        cascalog.logic.ops
        cascalog.logic.options
        cascalog.logic.predmacro
        cascalog.logic.testing
        cascalog.logic.vars
        cascalog.logic.zip
        cascalog.in-memory.join
        cascalog.in-memory.platform
        cascalog.in-memory.testing
        cascalog.in-memory.tuple
        cascalog.in-memory.util
        cascalog.api
        cascalog.playground]
  
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :jar-exclusions [#"\.java$"]
  :repositories {"conjars" "http://conjars.org/repo/"}
  :exclusions [log4j/log4j org.slf4j/slf4j-log4j12]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.macro "0.1.2"]
                 [log4j "1.2.16"]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]
                 [cascading/cascading-hadoop2-tez "3.0.0-wip-116"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-common
                               org.apache.hadoop/hadoop-hdfs
                               org.apache.hadoop/hadoop-mapreduce-client-core
                               org.apache.hadoop/hadoop-aws]]
                 [com.twitter/chill-hadoop "0.3.5"]
                 [com.twitter/carbonite "1.4.0"]
                 [com.twitter/maple "0.2.2"]
                 [prismatic/schema "0.3.7"
                  :exclusions [org.clojure/clojurescript]]
                 [jackknife "0.1.7"]
                 [hadoop-util "0.3.0"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-client ~HADOOP-VERSION]
                                       [org.apache.tez/tez-api "0.6.0"]
                                       [org.apache.tez/tez-mapreduce "0.6.0"]
                                       [org.apache.tez/tez-dag "0.6.0"]]}
             :dev {:resource-paths ["dev"]
                   :plugins [[lein-midje "3.1.3"]]
                   :injections [(require 'schema.core)
                                (schema.core/set-fn-validation! true)]
                   :dependencies
                   [[cascalog/midje-cascalog ~VERSION]]}})
