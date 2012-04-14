package jcascalog;

import cascalog.Util;
import clojure.lang.Keyword;
import java.util.ArrayList;
import java.util.List;

public class Predicate {
    List<Object> _pred = new ArrayList<Object>();
    
    
    public Predicate(Object op, Fields defaultFields) {
        addOp(_pred, op);
        _pred.addAll(defaultFields);
    }

    public Predicate(Object op, Fields infields, Fields outFields) {
        addOp(_pred, op);
        _pred.addAll(infields);
        _pred.add(Keyword.intern(">"));
        _pred.addAll(outFields);
    }
    
    public List<Object> toCascalogPredicate() {
        return _pred;
    }
    
    private static void addOp(List<Object> pred, Object op) {
        if(op instanceof ClojureOp) {
            ClojureOp cop = (ClojureOp) op;
            pred.add(Util.getVar(cop.getNamespace(), cop.getName()));
            if(cop.getHofArgs()!=null) {
                pred.add(cop.getHofArgs());
            }
        } else {
            pred.add(op);
        }
    }
}
