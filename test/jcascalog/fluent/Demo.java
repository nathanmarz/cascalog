package jcascalog.fluent;

import static jcascalog.fluent.JCascalog.query;
import static jcascalog.fluent.JCascalog.resultsFrom;
import static jcascalog.fluent.op.BooleanOps.gt;
import static jcascalog.fluent.op.NonBooleanOps.multiply;
import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import jcascalog.Playground;
import jcascalog.fluent.query.PredicateContainer;

import org.junit.Test;

// TODO project configuration : should be set to 1.6 compatibility (and not 1.5) and should
// include src/jvm as a source folder in eclipse...
// TODO nice to have : the query represented in clojure as a way to debug and learn the clojure way
public class Demo {

	/**
	 * <pre>
	 * (?<- (stdout) [?name] (full-names _ ?name))
	 * </pre>
	 */
	@Test
	public void shouldHandleQueryWithOnlyASourcePredicate() {
		// given
		final PredicateContainer query = query("?name") //
				.pred(Playground.FULL_NAMES, "_", "?name");

		// when
		final List<List<?>> encapsulatedNames = resultsFrom(query);

		// then
		assertThat(flatten(encapsulatedNames)).isEqualTo(//
				Arrays.asList("A B C D E",//
						"Alice Smith",//
						"Bobby John Johnson", //
						"CHRIS",//
						"Emily Buchanan",//
						"George Jett"));
	}

	/**
	 * <pre>
	 * (?<- (stdout) [?person ?age ?double-age]
	 * 					(age ?person ?age)
	 * 					(* ?age 2 :> ?double-age))
	 * </pre>
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void shouldHandleOutFields() {
		// given
		final PredicateContainer query = query("?person", "?age", "?double-age")
				.pred(Playground.AGE, "?person", "?age")
				.pred(multiply(), "?age", 2).out("?double-age");

		// when
		final List<List<?>> encapsulatedResults = resultsFrom(query);

		// then
		assertThat(flatten(encapsulatedResults)).isEqualTo(//
				Arrays.asList("david", 25L, 50L,//
						"emily", 25L, 50L, //
						"kumar", 27L, 54L, //
						"alice", 28L, 56L, //
						"gary", 28L, 56L, //
						"george", 31L, 62L, //
						"bob", 33L, 66L, //
						"luanne", 36L, 72L, //
						"chris", 40L, 80L));
	}

	/**
	 * <pre>
	 * (let [subquery (<- [?person] (age ?person ?age) (> ?age 35))]
	 * 		(?<- (stdout) [?name] (subquery ?person) (full-names ?person ?name))
	 * )
	 * </pre>
	 */
	@Test
	public void shouldHandleNestedQueries() {
		// given
		final PredicateContainer subQuery = query("?person")//
				.pred(Playground.AGE, "?person", "?age")//
				.pred(gt(), "?age", 35);

		final PredicateContainer query = query("?name")//
				.pred(subQuery, "?person")//
				.pred(Playground.FULL_NAMES, "?person", "?name");
		// when
		final List<List<?>> encapsulatedNames = resultsFrom(query);
		// then
		assertThat(flatten(encapsulatedNames)).isEqualTo(//
				Arrays.asList("CHRIS"));
	}

	private List<?> flatten(List<List<?>> tuples) {
		if (tuples.isEmpty()) {
			return Collections.emptyList();
		}
		final List<Object> flatList = new ArrayList<Object>(tuples.size()
				* tuples.get(0).size());
		for (List<?> tuple : tuples) {
			for (Object element : tuple) {
				flatList.add(element);
			}
		}
		return flatList;
	}

}
