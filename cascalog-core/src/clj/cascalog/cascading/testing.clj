(ns cascalog.cascading.testing
  (:require [clojure.test :refer :all]
            [cascalog.api :refer :all]
            [cascalog.logic.testing :as t]
            [cascalog.cascading.io :as io]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.platform :refer (normalize-sink-connection)]
            [jackknife.core :as u]
            [jackknife.seq :refer (unweave multi-set)]
            [schema.core :as s])
  (:import [cascalog.cascading.types CascadingPlatform]
           [java.io File]
           [cascading.tuple Fields]))

;; ## Cascading Testing Functions
;;
;; The following functions create proxies for dealing with various
;; output collectors.

(defn mk-test-tap [fields-def path]
  (-> (tap/sequence-file fields-def)
      (tap/lfs path)))

(letfn [(mapify-spec [spec]
          (if (map? spec)
            spec
            {:fields Fields/ALL :tuples spec}))]

  (defn mk-test-sink [spec path]
    (mk-test-tap (:fields (mapify-spec spec)) path)))

(defn- mk-tmpfiles+forms [amt]
  (let [tmpfiles  (take amt (repeatedly (fn [] (gensym "tap"))))
        tmpforms  (->> tmpfiles
                       (mapcat (fn [f]
                                 [f `(File.
                                      (str (io/temp-dir ~(str f))
                                           "/"
                                           (u/uuid)))])))]
    [tmpfiles (vec tmpforms)]))

(extend-protocol t/ITestable
  CascadingPlatform
  (process?- [_ bindings]
    (let [[ll :as bindings] bindings
          [log-level bindings] (if (contains? io/log-levels ll)
                                 [ll (rest bindings)]
                                 [:fatal bindings])]
      (io/with-log-level log-level
          (io/with-fs-tmp [_ sink-path]
            (with-job-conf {"io.sort.mb" 10}
              (let [bindings (mapcat (partial apply normalize-sink-connection)
                                     (partition 2 bindings))
                    [specs rules]  (unweave bindings)
                    sinks          (map mk-test-sink specs
                                        (u/unique-rooted-paths sink-path))
                    _              (apply ?- (interleave sinks rules))
                    out-tuples     (doall (map tap/get-sink-tuples sinks))]
                [specs out-tuples])))))))

(defn check-tap-spec [tap spec]
  (t/is-tuplesets= (tap/get-sink-tuples tap) spec))

(defn check-tap-spec-sets [tap spec]
  (is (= (multi-set (map set (t/doublify (tap/get-sink-tuples tap))))
         (multi-set (map set (t/doublify spec))))))

(defn with-expected-sinks-helper [checker bindings body]
  (let [[names specs] (map vec (unweave bindings))
        [tmpfiles tmpforms] (mk-tmpfiles+forms (count names))
        tmptaps (mapcat (fn [n t s]
                          [n `(cascalog.cascading.testing/mk-test-sink ~s ~t)])
                        names tmpfiles specs)]
    `(cascalog.cascading.io/with-tmp-files ~tmpforms
       (let [~@tmptaps]
         ~@body
         (dorun (map ~checker ~names ~specs))))))

;; bindings are name spec, where spec is either {:fields :tuples} or
;; vector of tuples
(defmacro with-expected-sinks [bindings & body]
  (with-expected-sinks-helper check-tap-spec bindings body))

(defmacro with-expected-sink-sets [bindings & body]
  (with-expected-sinks-helper check-tap-spec-sets bindings body))
