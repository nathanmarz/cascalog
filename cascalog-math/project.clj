(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-math VERSION
  :description "Math modules for Cascalog."
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:provided {:dependencies
                        [[cascalog/cascalog-core :version]]}
             :dev {:dependencies
                   [[cascalog/midje-cascalog :version]
                    [net.sourceforge.parallelcolt/parallelcolt "0.10.0"]]}})
