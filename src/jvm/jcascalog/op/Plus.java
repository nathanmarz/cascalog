package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Plus extends ClojureOp implements NonBooleanOp {
    public Plus() {
        super("clojure.core", "+");
    }
}
