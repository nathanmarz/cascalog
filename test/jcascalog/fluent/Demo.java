package jcascalog.fluent;

import static jcascalog.Api.juxt;
import static jcascalog.Playground.AGE;
import static jcascalog.Playground.FOLLOWS;
import static jcascalog.Playground.FULL_NAMES;
import static jcascalog.fluent.JCascalog.query;
import static jcascalog.fluent.JCascalog.resultsFrom;
import static jcascalog.fluent.op.BooleanOps.gt;
import static jcascalog.fluent.op.NonBooleanOps.minus;
import static jcascalog.fluent.op.NonBooleanOps.multiply;
import static jcascalog.fluent.op.NonBooleanOps.plus;
import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import jcascalog.fluent.query.PredicateContainer;

import org.junit.Test;

import clojure.lang.Var;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

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
				.pred(FULL_NAMES, "_", "?name");

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
				.pred(AGE, "?person", "?age")
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
				.pred(AGE, "?person", "?age")//
				.pred(gt(), "?age", 35);

		final PredicateContainer query = query("?name")//
				.pred(subQuery, "?person")//
				.pred(FULL_NAMES, "?person", "?name");

		// when
		final List<List<?>> encapsulatedNames = resultsFrom(query);

		// then
		assertThat(flatten(encapsulatedNames)).isEqualTo(//
				Arrays.asList("CHRIS"));
	}

	/**
	 * <pre>
	 * (def juxted (juxt + -))
	 * (?<- (stdout) [?person1 ?person2 ?knowledge ?gap]
	 * 					(age ?person1 ?age1) (age ?person2 ?age2)
	 * 					(follows ?person1 ?person2)
	 * 					(juxted ?age1 ?age2 :> ?knowledge ?gap)
	 * 					(> ?knowledge 60))
	 * </pre>
	 */	
	@Test
	@SuppressWarnings("unchecked")
	public void shouldHandleMultipleOutfields() {
		// given
		Object juxted = new PlusMinus();
		// Object juxted = juxt(plus(), minus());
		final PredicateContainer query = //
		query("?person1", "?person2", "?knowledge", "?gap")
				.pred(AGE, "?person1", "?age1")
				.pred(AGE, "?person2", "?age2")
				.pred(FOLLOWS, "?person1", "?person2")
				.pred(juxted, "?age1", "?age2")
				.out("?knowledge", "?gap")
				.pred(gt(), "?knowledge", 60);

		// when
		final List<List<?>> encapsulatedResults = resultsFrom(query);

		// then
		assertThat(flatten(encapsulatedResults)).isEqualTo(//
				Arrays.asList("david","luanne", 61, -11,//
						"alice", "bob", 61, -5, //
						"bob", "luanne", 69, -3, //
						"bob", "george", 64, 2, //
						"luanne", "gary", 64, 8));
	}
	
	private static class PlusMinus extends CascalogFunction {
		private static final long serialVersionUID = 1L;

		public void operate(FlowProcess flowProcess, FunctionCall fnCall) {
			Integer a = fnCall.getArguments().getInteger(0);
			Integer b = fnCall.getArguments().getInteger(1);
	    	fnCall.getOutputCollector().add(new Tuple(a+b,a-b));
	    }
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
