package jcascalog;

import java.util.Arrays;
import java.util.List;

public class PredicateMacro {
    Fields _inFields;
    Fields _outFields;
    List<Predicate> _preds;
    
    public PredicateMacro(Fields inFields, Fields outFields, Predicate... preds) {
        this(inFields, outFields, Arrays.asList(preds));
    }

    public PredicateMacro(Fields inFields, Fields outFields, List<Predicate> preds) {
        _inFields = inFields;
        _outFields = outFields;
        _preds = preds;
    }
    
    public Fields getInFields() {
        return _inFields;
    }
    
    public Fields getOutFields() {
        return _outFields;
    }
    
    public List<Predicate> getPredicates() {
        return _preds;
    }
}
