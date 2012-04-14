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
    
    public List<Object> toCascalogPredicate() {
        List<Object> pred = new ArrayList<Object>();
        addOp(pred, _op);
        pred.addAll(_fieldsDeclaration);
        return pred;
    }
    
    private void addOp(List<Object> pred, Object op) {
        pred.add(op);
        if(_hofArgs!=null) {
            pred.add(_hofArgs);
        }
    }
}
