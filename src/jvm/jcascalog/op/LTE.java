package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.BooleanOp;

public class LTE extends ClojureOp implements BooleanOp {
    public LTE() {
        super("clojure.core", "<=");
    }
}
