(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-core VERSION
  :description "Cascalog core libraries."
  :jvm-opts ["-Xmx768m"
             "-server"
             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :jar-exclusions [#"\.java$"]
  :exclusions [log4j/log4j org.slf4j/slf4j-log4j12]
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[cascading/cascading-hadoop "_"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core]]
                 [com.twitter/chill-hadoop "0.3.5"]
                 [com.twitter/carbonite "1.4.0"]
                 [com.twitter/maple "0.2.2"]
                 [org.clojure/clojure "_"]
                 [org.clojure/tools.macro "0.1.2"]
                 [org.slf4j/slf4j-log4j12 "_"]
                 [prismatic/schema "_" :exclusions [org.clojure/clojurescript]]
                 [jackknife "_"]
                 [log4j "_"]
                 [hadoop-util "_"]]
  :profiles
  {:dev {:dependencies [[cascalog/midje-cascalog :version]]
         :resource-paths ["dev"]
         :injections [(require 'schema.core)
                      (schema.core/set-fn-validation! true)]}})
