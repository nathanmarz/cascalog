package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.BooleanOp;

public class Equals extends ClojureOp implements BooleanOp {
    public Equals() {
        super("clojure.core", "=");
    }
}
