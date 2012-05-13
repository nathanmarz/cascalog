package jcascalog.fluent;

import java.util.Collections;
import java.util.List;

import jcascalog.Api;
import jcascalog.fluent.query.PredicateContainer;
import jcascalog.fluent.query.QueryBuilder;
import cascading.tap.Tap;
import clojure.lang.IteratorSeq;

public class JCascalog {

	public static QueryBuilder query(Object... fields) {
		return new QueryBuilder(fields);
	}

	public static void execute(Tap sink, PredicateContainer query) {
		Api.execute(sink, query.build());
	}

	/**
	 * For local execution ie test.
	 */
	@SuppressWarnings("unchecked")
	public static List<List<?>> resultsFrom(PredicateContainer query) {
		List<?> args = Collections.singletonList(query.build());
		return ((List<List<List<?>>>) Api.getApiFn("??-").applyTo(
				IteratorSeq.create(args.iterator()))).get(0);
	}

}
