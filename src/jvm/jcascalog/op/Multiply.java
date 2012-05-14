package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Multiply extends ClojureOp implements NonBooleanOp {
    public Multiply() {
        super("clojure.core", "*");
    }
}
