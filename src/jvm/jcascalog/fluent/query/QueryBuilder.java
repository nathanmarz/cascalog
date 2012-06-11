package jcascalog.fluent.query;

import java.util.LinkedList;
import java.util.List;

import jcascalog.Fields;
import jcascalog.Predicate;
import jcascalog.Subquery;
import jcascalog.fluent.JCascalog;
import jcascalog.fluent.op.BooleanOp;
import jcascalog.fluent.op.NonBooleanOp;
import jcascalog.fluent.op.SpecialOp;
import cascading.tap.Tap;

/**
 * Fluent builder for {@link JCascalog} queries.
 */
public class QueryBuilder implements PredicateContainer, OutCollector {
	private final Fields finalFields;
	private final List<Predicate> predicates = new LinkedList<Predicate>();
	
	private Object backupOp;
	private Fields backupInFields;
	
	public QueryBuilder(Object...fields) {
		this.finalFields = new Fields(fields);
	}
	
	private void backupPreviousPredicate() {
		if (backupOp != null) {
			predicates.add(new Predicate(backupOp, backupInFields));
		}
		backupOp = null;
		backupInFields = null;
	}
	
	private QueryBuilder addPartialPredicate(Object op, Object... fields) {
		backupPreviousPredicate();
		this.backupOp = op;
		backupInFields = new Fields(fields);
		return this;
	}
	
	private PredicateContainer addFullPredicate(Object op, Object...fields) {
		backupPreviousPredicate();
		predicates.add(new Predicate(op, new Fields(fields)));
		return this;
	}

	@Override
	public QueryBuilder pred(Object op, Object... fields) {
		return addPartialPredicate(op, fields);
	}
	
	
	@Override
	public PredicateContainer pred(Predicate predicate) {
		backupPreviousPredicate();
		predicates.add(predicate);
		return this;
	}
	
	@Override
	public PredicateContainer pred(List<?> source, Object... fields) {
		return addFullPredicate(source, fields);
	}

	@Override
	public PredicateContainer pred(Tap source, Object... fields) {
		return addFullPredicate(source, fields);
	}

	@Override
	public PredicateContainer pred(PredicateContainer source, Object... fields) {
		return addFullPredicate(source.build(), fields);
	}
	
	public QueryBuilder pred(BooleanOp booleanOp, Object... fields) {
		return addPartialPredicate(booleanOp, fields);
	}
	
	public OutCollector pred(NonBooleanOp nonBooleanOp, Object... fields) {
		return addPartialPredicate(nonBooleanOp, fields);
	}
	
	public PredicateContainer pred(SpecialOp specialOp, Object... fields) {
		return addFullPredicate(specialOp, fields);
	}

	@Override
	public PredicateContainer out(Object... fields) {
		if(backupOp == null) {
			throw new IllegalStateException("addPartialPredicate(op,fields) should have been called before.");
		}
		predicates.add(new Predicate(backupOp, backupInFields, new Fields(fields)));
		backupOp = null;
		backupInFields = null;
		return this;
	}

	@Override
	public Subquery build() {
		backupPreviousPredicate();
		return new Subquery(finalFields, predicates);
	}


}
