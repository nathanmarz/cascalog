package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.SpecialOp;

public class DistinctCount extends ClojureOp implements SpecialOp {
    public DistinctCount() {
        super("cascalog.ops", "distinct-count");
    }
}
