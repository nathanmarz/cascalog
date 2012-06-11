package jcascalog.fluent;


import jcascalog.fluent.query.PredicateContainer;
import jcascalog.fluent.query.QueryBuilder;
import jcascalog.fluent.runner.QueriesRunner;
import jcascalog.fluent.runner.TapsAndQueriesRunner;
import cascading.tap.Tap;

/**
 * Main entry point if you want to use cascalog with java.
 * See {@link Demo} for examples.
 */
public class JCascalog {

	/**
	 * Builds your query.
	 */
	public static QueryBuilder query(Object... fields) {
		return new QueryBuilder(fields);
	}
	
	/**
	 * Prepare running of queries with their taps.
	 */
	public static TapsAndQueriesRunner prepare() {
		return new TapsAndQueriesRunner();
	}
	
	/**
	 * Prepare running of queries with their taps, with a specified name.
	 */
	public static TapsAndQueriesRunner prepare(final String name) {
		return new TapsAndQueriesRunner(name);
	}
	
	/**
	 * Prepare running of queries with their taps.<br/>
	 * Shortcut version of <code>JCascalog.prepare().add(tap,query)</code>
	 */
	public static TapsAndQueriesRunner prepareWith(final Tap tap, final PredicateContainer query) {
		return new TapsAndQueriesRunner(tap,query);
	}
	
	/**
	 * Prepare running of queries with their taps.
	 */
	public static TapsAndQueriesRunner prepare(final String name, final Tap tap, final PredicateContainer query) {
		return new TapsAndQueriesRunner(name,tap,query);
	}
	
	/**
	 * Prepare running of queries and getting the results back.
	 */
	public static QueriesRunner prepareResults() {
		return new QueriesRunner();
	}
	
	/**
	 * Prepare running of queries and getting the results back.<br/>
	 * Shortcut version of <code>JCascalog.prepareResults().add(tap,query)</code>
	 */
	public static QueriesRunner prepareResultsFrom(final PredicateContainer query) {
		return new QueriesRunner(query);
	}

}
