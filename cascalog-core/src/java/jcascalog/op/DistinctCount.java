package jcascalog.op;

import jcascalog.ClojureOp;

public class DistinctCount extends ClojureOp {
  public DistinctCount() {
    super("cascalog.logic.ops", "distinct-count");
  }
}
