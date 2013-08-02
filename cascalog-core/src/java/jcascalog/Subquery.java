package jcascalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cascalog.Util;

public class Subquery {
  List<Predicate> _preds = new ArrayList<Predicate>();
  Fields _outFields;
  Predicate _currPred = null;

  public Subquery(String... fields) {
    this(Arrays.asList(fields));
  }

  public Subquery(List<String> fields) {
    _outFields = new Fields((List) fields);
  }

  public Fields getOutputFields() {
    return _outFields;
  }

  public Object getCompiledSubquery() {
    return Util.bootSimpleFn("cascalog.logic.parse", "parse-subquery")
      .invoke(_outFields, _preds);
  }

  public Subquery predicate(Object op, Object... fields) {
    return predicate(op, Arrays.asList(fields));
  }

  public Subquery predicate(Object op, List<Object> fields) {
    _currPred = new Predicate(op, fields);
    _preds.add(_currPred);
    return this;
  }

  public Subquery predicate(Predicate p) {
    _preds.add(p);
    return this;
  }

  public Subquery out(Object... fields) {
    return out(Arrays.asList(fields));
  }

  public Subquery out(List<Object> fields) {
    if (_currPred == null) {
      throw new RuntimeException("Cannot declare outfields for no predicate");
    } else {
      _currPred._outFields = fields;
      _currPred = null;
      return this;
    }
  }
}
