(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-checkpoint VERSION
  :description "Workflow checkpoints for the masses."
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[jackknife "_"]
                 [hadoop-util "_"]]
  :profiles {:provided {:dependencies
                        [[cascalog/cascalog-core :version]]}})
