package jcascalog;

import java.util.Arrays;
import java.util.List;

public class Subquery {
    Fields _outFields;
    List<Predicate> _preds;
    
    public Subquery(Fields outFields, Predicate... preds) {
        this(outFields, Arrays.asList(preds));
    }

    public Subquery(Fields outFields, List<Predicate> preds) {
        _outFields = outFields;
        _preds = preds;
    }
    
    public Fields getOutFields() {
        return _outFields;
    }
    
    public List<Predicate> getPredicates() {
        return _preds;
    }
}
