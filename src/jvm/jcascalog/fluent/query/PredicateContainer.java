package jcascalog.fluent.query;

import java.util.List;

import jcascalog.Predicate;
import jcascalog.Subquery;
import jcascalog.fluent.op.BooleanOp;
import jcascalog.fluent.op.NonBooleanOp;
import jcascalog.fluent.op.SpecialOp;
import cascading.tap.Tap;
import cascalog.MemorySourceTap;

public interface PredicateContainer {
	
	/**
	 * Generic without type help.
	 */
	public QueryBuilder pred(Object op, Object... fields);
	
	/**
	 * Generic without type help, useful for option.
	 */
	public PredicateContainer pred(Predicate predicate);
	
	/**
	 * Local list can be used as a {@link MemorySourceTap}.
	 */
	public PredicateContainer pred(List<?> source, Object... fields);
	
	/**
	 * Cascading {@link Tap} as source.
	 */
	public PredicateContainer pred(Tap source, Object... fields);
	
	/**
	 * Other query  as source.
	 */
	public PredicateContainer pred(PredicateContainer source, Object... fields);
	
	/**
	 * Boolean op may or may not have out fields.
	 */
	public QueryBuilder pred(BooleanOp booleanOp, Object... fields);
	
	/**
	 * Non boolean op must have out fields.
	 */
	public OutCollector pred(NonBooleanOp nonBooleanOp, Object... fields);
	
	/**
	 * Special op do not use any out fields.
	 */
	public PredicateContainer pred(SpecialOp specialOp, Object... fields);
	
	/**
	 * End the building and create the usable query.
	 */
	public Subquery build();

}
