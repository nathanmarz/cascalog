package jcascalog.op;

import jcascalog.ClojureOp;

public class Sum extends ClojureOp {
  public Sum() {
    super("cascalog.logic.ops", "sum");
  }
}
