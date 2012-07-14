package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.BooleanOp;

public class LT extends ClojureOp implements BooleanOp {
    public LT() {
        super("clojure.core", "<");
    }
}
