package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.BooleanOp;

public class GTE extends ClojureOp implements BooleanOp {
    public GTE() {
        super("clojure.core", ">=");
    }
}
