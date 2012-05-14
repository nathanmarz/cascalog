package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Minus extends ClojureOp implements NonBooleanOp {
    public Minus() {
        super("clojure.core", "-");
    }
}
