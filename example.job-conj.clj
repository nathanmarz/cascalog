;; ## job-conf.clj
;;
;; This is example job-conf.clj file, meant to provide default
;;settings to all queries executed inside this project. To get
;;started, create a file called "job-conf.clj" inside the "resources"
;;directory at your project's root. (this is called "resources" by
;;default, though you can customize this in project.clj with the
;;following k-v pair:
;;
;;    :resources-path "confdir"
;;
;; job-conf.clj must end with a job-conf map. Feel free to define
;; functions, import namespaces and evaluate code above the final
;; return form.
;;
;; Here we import Hadoop's java serialization interface:
(import 'org.apache.hadoop.io.serializer.JavaSerialization)

;; And here we import a Thrift serialization. Get this by including
;;
;;    [backtype/cascading-thrift "0.1.0"]
;;
;; As a dependency.

(import 'backtype.hadoop.ThriftSerialization)

;; Now, the job-conf map:
{"io.serializations" JavaSerialization}

;; To provide multiple arguments, skip the usual comma separation and
;; wrap multiple arguments in a vector:
{"io.serializations" [ThriftSerialization JavaSerialization]}

;; The above examples use class symbols directly. You can also use
;; string versions of the full qualified class names. (That's it!
;; We're ending with the job-conf map below. Nothing may come after,
;; not even comments.)

{"io.serializations" ["backtype.hadoop.ThriftSerialization"
                      "org.apache.hadoop.io.serializer.JavaSerialization"]}
