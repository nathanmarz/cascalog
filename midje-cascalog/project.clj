(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/midje-cascalog VERSION
  :description "Cascalog functions for Midje."
  :plugins [[lein-modules "0.3.11"]]
  :dependencies [[midje "1.5.1" :exclusions [org.clojure/clojure]]]
  :profiles
  {:provided {:dependencies
              [[cascalog/cascalog-core :version]]}})
