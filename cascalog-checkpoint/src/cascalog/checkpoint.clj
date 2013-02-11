(ns cascalog.checkpoint
  "Alpha!"
  (:use [cascalog.api :only [with-job-conf]])
  (:require [hadoop-util.core :as h]
            [cascalog.conf :as conf]
            [jackknife.core :as u]
            [jackknife.seq :as seq])
  (:import [java.util Collection]
           [org.apache.log4j Logger]
           [java.util.concurrent Semaphore]
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]))

(comment
  "Example usage."
  (workflow ["/tmp/checkpoints"]
           aaa ([:tmp-dirs "/tmp/.../shredded"]
                  (run-shredder!)
                  (consolidate ...))
           bbb ([:deps nil] )
           ccc ([:deps [aaa bbb]])))

(defstruct WorkflowNode ::tmp-dirs ::fn ::deps)
(defstruct Workflow ::fs ::graph-atom ::checkpoint-dir ::last-node-atom)

(defn mk-workflow [checkpoint-dir]
  (let [fs (h/filesystem)]
    (h/mkdirs fs checkpoint-dir)
    (struct Workflow fs (atom {}) checkpoint-dir (atom nil))))

(defn add-component*
  "Ideal version of this will take the tmp-dirs symbols and slap them
  onto the end of the base directory, removing the need to manually
  bind directories."
  [workflow name afn & {:keys [tmp-dirs deps] :or {deps :last}}]
  (let [last-node  @(::last-node-atom workflow)
        graph-atom (::graph-atom workflow)
        deps       (case deps
                     :last (if last-node [last-node] [])
                     :all  (keys @graph-atom)
                     (when deps
                       (seq/collectify deps)))
        tmp-dirs   (when tmp-dirs
                     (seq/collectify tmp-dirs))]
    (when (contains? @graph-atom name)
      (u/throw-illegal (str name " already exists in workflow")))
    (swap! graph-atom assoc name (struct WorkflowNode tmp-dirs afn deps))
    (reset! (::last-node-atom workflow) name)
    name))

(defn- mk-runner
  [fs token node status-atom sem log]
  (let [config conf/*JOB-CONF*]
    (Thread.
     (fn []
       (with-job-conf config
         (try (if-not (.exists fs (h/path token))
                (do (doseq [t (::tmp-dirs node)]
                      (h/delete fs t true))
                    ((::fn node))
                    (when-not (.createNewFile fs (h/path token))
                      (u/throw-runtime
                       (str "Unable to make checkpoint token " token))))
                (.info log (str "Skipping " token "...")))
              (reset! status-atom :successful)
              (catch Throwable t
                (.error log "Component failed" t)
                (reset! status-atom :failed))
              (finally (.release sem))))))))

(defn- fail-workflow!
  [log nodes-map]
  (let [nodes (vals nodes-map)
        running-nodes (filter #(= :running @(::status %)) nodes)
        threads (map ::runner-thread nodes)]
    (.info log "Workflow failed - interrupting components")
    (doseq [t threads] (.interrupt t))
    (.info log "Waiting for running components to finish")
    (doseq [t threads] (.join t))
    (u/throw-runtime "Workflow failed")))

(defn exec-workflow! [workflow]
  (let [fs (h/filesystem)
        log (Logger/getLogger "checkpointed-workflow")
        sem (Semaphore. 0)
        nodes (into {}
                    (for [[k v] @(::graph-atom workflow)
                          :let [status-atom (atom :unstarted)]]
                      [k (assoc v
                           ::status status-atom
                           ::runner-thread (mk-runner
                                            fs
                                            (str (::checkpoint-dir workflow)
                                                 "/" k)
                                            v
                                            status-atom
                                            sem
                                            log))]))]
    (loop []
      (doseq [[name node] nodes]
        (when (and (= :unstarted @(::status node))
                   (every? (fn [[_ n]] (= :successful @(::status n)))
                           (select-keys nodes (::deps node))))
          (reset! (::status node) :running)
          (.start (::runner-thread node))))
      (.acquire sem)
      (let [statuses (set (map (fn [[_ n]] @(::status n)) nodes))]
        (cond (contains? statuses :failed) (fail-workflow! log nodes)
              (some #{:running :unstarted} statuses) (recur)
              :else (.info log "Workflow completed successfully"))))
    (h/delete fs (::checkpoint-dir workflow) true)))

(defmacro component [workflow name kwargs & body]
  `(add-component* ~workflow ~name (fn [] ~@body) ~@kwargs))

(defn build-dir-bindings
  "Constructs a sequence of alternating symbols and path strings,
  given a sequence of workflow bindings and a base checkpoint
  directory. Example result:

  (some-path \"/my/root/some-path\")"
  [checkpoint-dir bindings]
  (let [tmp-syms (->> (partition 2 bindings)
                      (mapcat (fn [[_ [kwd-form]]]
                                (when-let [dirseq (:tmp-dirs
                                                   (apply hash-map kwd-form))]
                                  (seq/collectify dirseq)))))]
    (mapcat (fn [sym]
              (let [s (str sym)]
                [sym `(str ~checkpoint-dir "/data/" ~s)]))
            tmp-syms)))

(defmacro workflow [[checkpoint-dir] & bindings]
  (let [workflow-sym (gensym "workflow")
        tmp-bindings (build-dir-bindings checkpoint-dir bindings)
        bindings (->> (partition 2 bindings)
                      (mapcat (fn [[name-sym code]]
                                [name-sym (concat [`component
                                                   workflow-sym
                                                   (str name-sym)]
                                                  code)])))]
    `(let [~workflow-sym (mk-workflow ~checkpoint-dir)
           ~@tmp-bindings
           ~@bindings]
       (exec-workflow! ~workflow-sym))))
