(defproject cascalog "1.10.1-SNAPSHOT"
  :min-lein-version "2.0.0"
  :plugins [[lein-sub "0.2.4"]]
  :sub [cascalog-core cascalog-checkpoint]
  :dependencies [[org.clojure/clojure "1.4.0"]])
