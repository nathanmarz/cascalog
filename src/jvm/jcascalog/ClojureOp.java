package jcascalog;

// TODO: implement as part of transformation to real predicates

import cascalog.Util;
import java.util.Arrays;
import java.util.List;

public class ClojureOp implements PredicateMacro {
    String _namespace;
    String _name;
    List<Object> _hofArgs;
    
    public ClojureOp(String namespace, String name, List<Object> hofArgs) {
        _namespace = namespace;
        _name = name;
        _hofArgs = hofArgs;
    }

    public ClojureOp(String namespace, String name) {
        this(namespace, name, null);
    }

    @Override
    public List<Predicate> getPredicates(Fields inFields, Fields outFields) {
        Predicate pred = new Predicate(Util.getVar(_namespace, _name), inFields, outFields);
        pred.setHofArgs(_hofArgs);
        return Arrays.asList(pred);
    }
}
