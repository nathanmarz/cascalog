package jcascalog;

import java.util.Collections;
import java.util.List;

import cascalog.Util;
import clojure.lang.IFn;
import clojure.lang.Var;

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

  public Object toVar() {
    Var ret = Util.getVar(_namespace, _name);
    if (!hofArgs.isEmpty())
      return ret.applyTo(Util.coerceToSeq(hofArgs));
    else
      return ret.deref();
  }
}
