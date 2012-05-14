package jcascalog.op;

import java.util.Arrays;
import jcascalog.ClojureOp;
import jcascalog.fluent.op.SpecialOp;

public class Limit extends ClojureOp implements SpecialOp {
    public Limit(int amt) {
        super("cascalog.ops", "limit", Arrays.asList((Object)amt));
    }
}
