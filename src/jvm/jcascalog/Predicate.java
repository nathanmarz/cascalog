package jcascalog;

import clojure.lang.Keyword;
import java.util.ArrayList;
import java.util.List;

public class Predicate {
    List<Object> _fieldsDeclaration = new ArrayList<Object>();
    Object _op;
    List<Object> _hofArgs = null;
    
    public Predicate(Object op, Fields defaultFields) {
        _op = op;
        _fieldsDeclaration.addAll(defaultFields);
    }

    public Predicate(Object op, Fields infields, Fields outFields) {
        _op = op;
        _fieldsDeclaration.addAll(infields);
        _fieldsDeclaration.add(Keyword.intern(">"));
        _fieldsDeclaration.addAll(outFields);
    }
    
    public void setHofArgs(List<Object> hofArgs) {
        _hofArgs = hofArgs;
    }
    
    public List<Object> toRawCascalogPredicate() {
        List<Object> pred = new ArrayList<Object>();
        pred.add(_op); // the op
        pred.add(null); // the "var"
        List<Object> hofAndFields = new ArrayList<Object>(_fieldsDeclaration);
        if(_hofArgs!=null) hofAndFields.add(0, _hofArgs);
        pred.add(hofAndFields);
        return pred;
    }    
}
