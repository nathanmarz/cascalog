package jcascalog.op;

import java.util.Arrays;

import jcascalog.ClojureOp;

public class Limit extends ClojureOp {
  public Limit(int amt) {
    super("cascalog.logic.ops", "limit", Arrays.asList((Object) amt));
  }
}
