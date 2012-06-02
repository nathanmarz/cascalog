package jcascalog.fluent.query;

public interface OutCollector {
	
	/**
	 * The following clojure predicate
	 * <pre>
	 * (whatever ?varin1 ?varin2 :> ?varout1 ?varout2)
	 * </pre>
	 * would be written in java as
	 * <pre>
	 * .pred(whatever, "?varin1", "?varin2").out("?varout1","?varout2")
	 * </pre>
	 */
	PredicateContainer out(Object... fields);

}
