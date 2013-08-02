package jcascalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cascalog.Util;
import clojure.lang.Keyword;

public class PredicateMacroTemplate {
  public static PredicateMacroTemplateBuilder build(String... inFields) {
    return build(Arrays.asList(inFields));
  }

  public static PredicateMacroTemplateBuilder build(List<String> inFields) {
    return new PredicateMacroTemplateBuilder(inFields);
  }

  public static class PredicateMacroTemplateBuilder {
    List<String> _inFields;

    public PredicateMacroTemplateBuilder(List<String> inFields) {
      _inFields = inFields;
    }

    public PredicateMacroTemplate out(String... outFields) {
      return out(Arrays.asList(outFields));
    }

    public PredicateMacroTemplate out(List<String> outFields) {
      return new PredicateMacroTemplate(_inFields, outFields);
    }
  }

  List<String> _inFields;
  List<String> _outFields;
  List<Predicate> _preds = new ArrayList<Predicate>();
  Predicate _currPred = null;

  public PredicateMacroTemplate(List<String> inFields, List<String> outFields) {
    _inFields = inFields;
    _outFields = outFields;
  }

  public PredicateMacroTemplate predicate(Object op, Object... fields) {
    return predicate(op, Arrays.asList(fields));
  }

  public PredicateMacroTemplate predicate(Object op, List<Object> fields) {
    _currPred = new Predicate(op, fields);
    _preds.add(_currPred);
    return this;
  }

  public PredicateMacroTemplate predicate(Predicate p) {
    _preds.add(p);
    return this;
  }

  public PredicateMacroTemplate out(Object... fields) {
    return out(Arrays.asList(fields));
  }

  public PredicateMacroTemplate out(List<Object> fields) {
    if (_currPred == null) {
      throw new RuntimeException("Cannot declare outfields for no predicate");
    } else {
      _currPred._outFields = fields;
      _currPred = null;
      return this;
    }
  }

  public Object getCompiledPredMacro() {
    return Util.bootSimpleFn("cascalog.logic.predmacro", "build-predmacro")
      .invoke(_inFields, _outFields, _preds);
  }
}
