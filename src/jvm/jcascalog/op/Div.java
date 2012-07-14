package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class Div extends ClojureOp implements NonBooleanOp {
    public Div() {
        super("cascalog.api", "div");
    }
}
