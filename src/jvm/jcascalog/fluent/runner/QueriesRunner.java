package jcascalog.fluent.runner;

import java.util.ArrayList;
import java.util.List;

import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.fluent.JCascalog;
import jcascalog.fluent.query.PredicateContainer;

/**
 * @see JCascalog#prepareResults()
 */
public class QueriesRunner {
	private final List<Subquery> subqueries = new ArrayList<Subquery>();
	
	public QueriesRunner() {
	}
	
	public QueriesRunner(PredicateContainer query) {
		add(query);
	}
	
	/**
	 * Add another query.
	 */
	public QueriesRunner add(PredicateContainer query) {
		subqueries.add(query.build());
		return this;
	}
	
	/**
	 * Runs all the provided queries and returns the results.
	 */
	public List<List<List<?>>> andRun() {
		return andPrivateRun();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<List<List<?>>> andPrivateRun() {
		return Api.executeAndReturnResults((List)subqueries);
	}

}
