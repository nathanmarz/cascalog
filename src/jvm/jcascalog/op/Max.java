package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Max extends ClojureOp implements NonBooleanOp {
    public Max() {
        super("cascalog.ops", "max");
    }
}
