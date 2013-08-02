package jcascalog;

import java.util.ArrayList;
import java.util.List;

import clojure.lang.Keyword;

public class Predicate {
  List<Object> _initialFields;
  List<Object> _outFields = null;
  Object _op;

  public Predicate(Object op, List<Object> initialFields) {
    _op = op;
    _initialFields = initialFields;
  }

  public Predicate(Object op, List<Object> inFields, List<Object> outFields) {
    _op = op;
    _initialFields = inFields;
    _outFields = outFields;
  }

  public List<Object> toRawCascalogPredicate() {
    List<Object> pred = new ArrayList<Object>();
    pred.add(_op);
    pred.addAll(_initialFields);
    if (_outFields != null) {
      pred.add(Keyword.intern(">"));
      pred.addAll(_outFields);
    }
    return pred;
  }
}
