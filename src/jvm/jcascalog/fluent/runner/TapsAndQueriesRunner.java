package jcascalog.fluent.runner;

import java.util.ArrayList;
import java.util.List;

import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.fluent.JCascalog;
import jcascalog.fluent.query.PredicateContainer;
import cascading.tap.Tap;

/**
 * @see JCascalog#prepare()
 */
public class TapsAndQueriesRunner {
	private final String name;
	private final List<Tap> taps = new ArrayList<Tap>();
	private final List<Subquery> subqueries = new ArrayList<Subquery>();
	
	public TapsAndQueriesRunner() {
		name = null;
	}

	public TapsAndQueriesRunner(String name) {
		this.name = name;
	}
	
	public TapsAndQueriesRunner(Tap tap, PredicateContainer query) {
		this();
		add(tap, query);
	}
	
	public TapsAndQueriesRunner(String name, Tap tap, PredicateContainer query) {
		this(name);
		add(tap, query);
	}
	
	/**
	 * Add a new couple tap/query to run together.
	 */
	public TapsAndQueriesRunner add(Tap tap, PredicateContainer query) {
		taps.add(tap);
		subqueries.add(query.build());
		return this;
	}
	
	/**
	 * Runs all the provided queries. If you want to get the results back,
	 * please see {@link JCascalog#prepareResults()} instead.
	 */
	public void andRun() {
		andPrivateRun();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void andPrivateRun() {
		// Api takes care of the name being nullable
		Api.execute(name, (List)taps, (List)subqueries);
	}
	
	

}
