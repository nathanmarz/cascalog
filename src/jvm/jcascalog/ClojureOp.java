package jcascalog;

import cascalog.Util;
import clojure.lang.RT;
import clojure.lang.Var;
import java.util.ArrayList;
import java.util.List;

public class ClojureOp {
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
    
    public List<Object> toRawCascalogPredicate(List<Object> fieldsDeclaration) {
        List<Object> pred = new ArrayList<Object>();
        Var v = Util.getVar(_namespace, _name);
        Object op = v;
        if(_hofArgs!=null) {
           op = v.applyTo(RT.seq(_hofArgs));
        }
        pred.add(op);
        pred.add(fieldsDeclaration);
        return pred;
    }  
}
