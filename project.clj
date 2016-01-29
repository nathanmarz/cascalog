(def VERSION (slurp "VERSION"))
(def MODULES (-> "MODULES" slurp (.split "\n")))
(def DEPENDENCIES (for [m MODULES] [(symbol (str "cascalog/" m)) VERSION]))

(eval `(defproject cascalog/cascalog ~VERSION
         :description "Hadoop without the Hassle."
         :url "http://www.cascalog.org"
         :license {:name "Eclipse Public License"
                   :url "http://www.eclipse.org/legal/epl-v10.html"}
         :mailing-list {:name "Cascalog user mailing list"
                        :archive "https://groups.google.com/d/forum/cascalog-user"
                        :post "cascalog-user@googlegroups.com"}
         :dependencies [~@DEPENDENCIES]
         :plugins [[~'lein-sub "0.3.0"]
                   [~'codox "0.6.6"]]
         :sub [~@MODULES]
         :codox {:src-dir-uri "http://github.com/nathanmarz/cascalog/blob/master"
                 :src-linenum-anchor-prefix "L"
                 :sources ["cascalog-core/src"
                           "cascalog-checkpoint/src"
                           "cascalog-more-taps/src"
                           "cascalog-math/src"
                           "cascalog-lzo/src"
                           "midje-cascalog/src"]}))
