package jcascalog.op;

import java.util.Arrays;
import jcascalog.ClojureOp;
import jcascalog.fluent.op.SpecialOp;

public class FixedSample extends ClojureOp implements SpecialOp {
    public FixedSample(int amt) {
        super("cascalog.ops", "fixed-sample-agg", Arrays.asList((Object)amt));
    }
}
