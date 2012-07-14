package jcascalog.op;

import java.util.Arrays;
import java.util.regex.Pattern;
import jcascalog.ClojureOp;
import jcascalog.fluent.op.NonBooleanOp;

public class ReParse extends ClojureOp implements NonBooleanOp {
    public ReParse(Pattern pattern) {
        super("cascalog.ops", "re-parse", Arrays.asList((Object)pattern));
    }
}
