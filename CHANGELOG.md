## 1.8.4

* Add project-wide jobconf settings in job-conf.clj.
* optional docstring and metadata support on defparallelagg and defparallelbuf.
* def*ops now present proper argument list metadata.
* Added cascalog.ops/partial
* fixed :sinkmode op
* defaggregateop no longer fails when returning nil
* Better error reporting on parametrized ops (variadic args aren't supported; this now throws an error.)
