package jcascalog.op;

import java.util.Arrays;
import jcascalog.ClojureOp;
import jcascalog.fluent.op.SpecialOp;

public class LimitRank extends ClojureOp implements SpecialOp {
    public LimitRank(int amt) {
        super("cascalog.ops", "limit-rank", Arrays.asList((Object)amt));
    }
}
