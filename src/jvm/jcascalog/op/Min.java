package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Min extends ClojureOp implements NonBooleanOp {
    public Min() {
        super("cascalog.ops", "min");
    }
}
