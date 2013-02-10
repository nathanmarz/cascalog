(def shared
  '[[cascalog "1.9.0"]
    [jackknife "0.1.2"]
    [hadoop-util "0.2.8"]])

(defproject cascalog/cascalog-checkpoint "0.2.0"
  :description "Workflow checkpoints for the masses."
  :dependencies ~(conj shared '[org.clojure/clojure "1.4.0"])
  :multi-deps {:all {:dependencies ~shared}
               :1.2 {:dependencies [[org.clojure/clojure "1.2.1"]]}
               :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}})
