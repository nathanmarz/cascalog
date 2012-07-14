package jcascalog.fluent;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions helping testing your use cases of {@link JCascalog}.
 *
 */
public class TestingJCascalog {

	/**
	 * Flattens the results obtained from running queries using {@link JCascalog}.<br/>
	 * <code>
	 * final List&lt?> results = flatten(prepareResultsFrom(query).andRun());
	 * </code>
	 */
	// XXX it is a shame to redefine it but it works
	public static List<?> flatten(List<List<List<?>>> listOfTuples) {
		final List<Object> results = new ArrayList<Object>();
		for (List<List<?>> tuples : listOfTuples) {
			for (List<?> tuple : tuples) {
				for (Object element : tuple) {
					results.add(element);
				}
			}
		}
		return results;
	}
}
