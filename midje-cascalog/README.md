## Midje-Cascalog

Midje-Cascalog is a thin layer over [midje](https://github.com/marick/Midje) that makes it easy and fun to test [Cascalog](https://github.com/nathanmarz/cascalog) queries! Scroll down for an in-depth example.

[Cascalog Testing 2.0](http://sritchie.github.com/2012/01/22/cascalog-testing-20/) gives a long discussion on various midje-cascalog idioms.

## Usage Instructions

To use midje-cascalog in your own project, add the following two entries to `:dev-dependencies` inside  of your `project.clj` file:

    [lein-midje "3.0.1"]
    [cascalog/midje-cascalog "3.0.0"]

Midje-Cascalog supports Clojure 1.3+ and Cascalog 1.8+. Add `(:use [midje sweet cascalog])` to your testing namespace to get started.

When you're all finished writing tests, `lein midje` at the command line will run all Midje tests and generate a summary.

## Example Query Test

Let's say you want to test a Cascalog workflow that examines your user datastore and returns the user with the greatest number of followers. Your workflow's top level query will generate a single tuple containing that user's name and follower-count. Here's the code:

    (defn max-followers-query [datastore-path]
      (let [src (name-vars (complex-subquery datastore-path)
                           ["?user" "?follower-count"])]
        (cascalog.ops/first-n src 1 :sort ["?follower-count"] :reverse true)))

`max-followers-query` is a function that returns a Cascalog subquery. It works like this:

* The function accepts a path, (`datastore-path`) and passes it into a function called `complex-subquery`.
* `complex-subquery` returns a subquery that generates 2-tuples; this subquery is passed into `name-vars`.
 * `name-vars` binds this subquery to `src` after naming its output variables `?user` and `?follower-count`.
* `first-n` returns a subquery that
  * sorts tuples from `src` in reverse order by follower count, and
  * returns a single 2-tuple with the name and follower-count of our most popular user.

At a high level, the subquery returned by =max-followers-query= is responsible for a single piece of application logic:

* extracting the tuple with max `?follower-count` from the tuples returned by `(complex-subquery datastore-path)`.

A correct test of `max-followers-query` will test this piece of logic in isolation.

```clj
    (fact "Query should return a single tuple containing
           [most-popular-user, follower-count]."
          (max-followers-query :path) => (produces [["richhickey" 2961]])
          (provided
            (complex-subquery :path) => [["sritchie09" 180]
                                         ["richhickey" 2961]]))
```

Midje circumvents all extra complexity by mocking out the result of `(complex-subquery datastore-path)` and forcing it to return a specific Clojure sequence of `[?user ?follower-count]` tuples.

`produces` checks result from queries. The fact passes if these statements are true and fails otherwise. The above fact states that

* when `max-followers-query` is called with the argument `:path`,
* it will produce `[[ richhickey" 2961]]`,
* provided `(complex-subquery :path)` produces `[["sritchie09" 180] ["richhickey" 2961]]`.

Fact-based testing separates application logic from the way data is stored. By mocking out `complex-subquery`, our fact tests `max-followers-query` in isolation and proves it correct for all expected inputs.

This approach is not just better than the "state of the art" of MapReduce testing, [as defined by Cloudera](http://www.cloudera.com/blog/2009/07/debugging-mapreduce-programs-with-mrunit/); it completely obliterates the old way of thinking, and makes it possible to build very complex workflows with a minimum of uncertainty.

Fact-based tests are the building blocks of rock-solid production workflows.
