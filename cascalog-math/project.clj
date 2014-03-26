(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def HADOOP-VERSION (-> ROOT-DIR (str "/../HADOOP-VERSION") slurp))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject cascalog/cascalog-math VERSION
  :description "Math modules for Cascalog."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core ~VERSION]
                                       [org.apache.hadoop/hadoop-core ~HADOOP-VERSION]]}
             :dev {:dependencies [[cascalog/midje-cascalog ~VERSION]
                                  [net.sourceforge.parallelcolt/parallelcolt "0.10.0"]]
                   :plugins [[lein-midje "3.1.3"]]}})
