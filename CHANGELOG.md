## 1.8.5

* Moved project configurations over to cascalog.
* Memory-Source-Tap now uses project settings from job-conf.clj (bugfix!)
* Kryo Serialization for clojure primitives, clojure collections and select java primitives.
* Functions defined with defmain are now callable by their supplied names. (Previously they weren't usable except to generate aot-compiled classes.)
* `bootstrap` and `bootstrap-emacs` are now functions, not macros.
* defops and defmain include :skip-wiki true in metadata, allowing them to be used with codox or autodoc and not generate clutter.
* Sets are no longer destructured as tuples by Cascalog.

In addition to previous support for the various number types + strings, the following classes are now serialized by default:

- **Java primitives & Collections**
  - java.math.BigDecimal
  - java.math.BigInteger
  - java.util.Date
  - java.util.UUID
  - java.sql.Date
  - java.sql.Time
  - java.sql.Timestamp
  - java.net.URI
  - java.util.ArrayList
  - java.util.HashMap
  - java.util.HashSet

- **Clojure Primitives**
  - clojure.lang.Keyword
  - clojure.lang.Symbol
  - clojure.lang.Ratio
  - clojure.lang.Var
  - clojure.lang.BigInt (only in Clojure 1.3)

- **Clojure Collections and sequences:**
  - clojure.lang.Cons
  - clojure.lang.IteratorSeq
  - clojure.lang.LazySeq
  - clojure.lang.MapEntry
  - clojure.lang.PersistentArrayMap
  - clojure.lang.PersistentHashMap
  - clojure.lang.PersistentHashSet
  - clojure.lang.PersistentList
  - clojure.lang.PersistentList$EmptyList
  - clojure.lang.PersistentStructMap
  - clojure.lang.PersistentVector
  - clojure.lang.StringSeq

## 1.8.4

* Add project-wide jobconf settings in job-conf.clj.
* optional docstring and metadata support on defparallelagg and defparallelbuf.
* def*ops now present proper argument list metadata.
* Added cascalog.ops/partial
* fixed :sinkmode docstring.
* defaggregateop no longer fails when returning nil
* Better error reporting on parametrized ops (variadic args aren't supported; this now throws an error.)
* MUCH better error messaging on failed tests. Here's the old way:

    expected: (= (map multi-set (map doublify set1)) (map multi-set (map doublify set2)))
      actual: (not (= ({[2.0] 1}) ({[1.0] 1})))

vs the new:

    expected: (= input output)
    actual: (not (= [[2.0]] [[1.0]]))
