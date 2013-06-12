package jcascalog;

import java.util.ArrayList;
import java.util.List;

import cascalog.Util;
import clojure.lang.Var;

public class ClojureOp {
  String _namespace;
  String _name;

  public ClojureOp(String namespace, String name) {
    _namespace = namespace;
    _name = name;
  }

  public Var toVar() {
    return Util.getVar(_namespace, _name);
  }
}
