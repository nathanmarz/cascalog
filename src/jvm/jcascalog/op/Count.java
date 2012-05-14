package jcascalog.op;

import jcascalog.ClojureOp;
import jcascalog.fluent.op.SpecialOp;

public class Count extends ClojureOp implements SpecialOp {
    public Count() {
        super("cascalog.ops", "count");
    }
}
