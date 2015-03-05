(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-more-taps VERSION
  :description "More taps for Cascalog"
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :plugins [[lein-modules "0.3.11"]]
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :jar-exclusions [#"\.java$"]
  :profiles {:provided {:dependencies [[cascalog/cascalog-core :version]]}
             :dev {:dependencies
                   [[cascalog/midje-cascalog :version]
                    [hadoop-util "_"]]}})
