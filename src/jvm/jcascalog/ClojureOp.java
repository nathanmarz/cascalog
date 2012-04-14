package jcascalog;

// TODO: implement as part of transformation to real predicates

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
    
    public String getNamespace() {
        return _namespace;
    }
    
    public String getName() {
        return _name;
    }
    
    public List<Object> getHofArgs() {
        return _hofArgs;
    }
    
    
}
