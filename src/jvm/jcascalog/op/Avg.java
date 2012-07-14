package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Avg extends ClojureOp implements NonBooleanOp {
    public Avg() {
        super("cascalog.ops", "avg");
    }
}
