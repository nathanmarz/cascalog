(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def HADOOP-VERSION (-> ROOT-DIR (str "/../HADOOP-VERSION") slurp))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-checkpoint VERSION
  :description "Workflow checkpoints for the masses."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :dependencies [[jackknife "0.1.7"]
                 [hadoop-util "0.3.0"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :dev {:plugins [[lein-midje "3.1.3"]]}
             :provided {:dependencies [[cascalog/cascalog-core ~VERSION]
                                       [org.apache.hadoop/hadoop-core ~HADOOP-VERSION]]}})
