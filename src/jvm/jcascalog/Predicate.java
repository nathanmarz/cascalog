package jcascalog;

import clojure.lang.Keyword;
import java.util.ArrayList;
import java.util.List;

public class Predicate {
    List<Object> _pred = new ArrayList<Object>();
    
    
    public Predicate(CascalogOp op, Fields defaultFields) {
        _pred.add(op);
        _pred.addAll(defaultFields);
    }

    public Predicate(CascalogOp op, Fields infields, Fields outFields) {
        _pred.add(op);
        _pred.addAll(infields);
        _pred.add(Keyword.intern(">"));
        _pred.addAll(outFields);
    }
    
    public List<Object> toCascalogPredicate() {
        return _pred;
    }
}
