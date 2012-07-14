package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.BooleanOp;

public class GT extends ClojureOp implements BooleanOp {
    public GT() {
        super("clojure.core", ">");
    }
}
