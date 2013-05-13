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
    List<Object> fieldsDeclaration = new ArrayList<Object>();
    fieldsDeclaration.addAll(_initialFields);
    if (_outFields != null) {
      fieldsDeclaration.add(Keyword.intern(">"));
      fieldsDeclaration.addAll(_outFields);
    }
    List<Object> pred = new ArrayList<Object>();
    pred.add(_op);
    pred.add(fieldsDeclaration);
    return pred;
  }
}
