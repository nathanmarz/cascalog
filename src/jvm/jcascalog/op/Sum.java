package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Sum extends ClojureOp implements NonBooleanOp {
    public Sum() {
        super("cascalog.ops", "sum");
    }
}
