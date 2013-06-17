package jcascalog;

import java.util.Collections;
import java.util.List;

import cascalog.Util;
import clojure.lang.IFn;

public class ClojureOp {
  String _namespace;
  String _name;
  final List<Object> hofArgs;

  public ClojureOp(String namespace, String name) {
    this(namespace, name, Collections.emptyList());
  }

  public ClojureOp(String namespace, String name, List<Object> hofArgs) {
    _namespace = namespace;
    _name = name;
    this.hofArgs = hofArgs;
  }

  public IFn toVar() {
    IFn ret = Util.getVar(_namespace, _name);
    if (!hofArgs.isEmpty())
      return (IFn) ret.applyTo(Util.coerceToSeq(hofArgs));
    else
      return ret;
  }
}
