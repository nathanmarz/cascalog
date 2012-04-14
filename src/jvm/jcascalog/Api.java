package jcascalog;

import cascading.flow.Flow;
import cascading.tap.Tap;
import cascalog.Util;
import clojure.lang.IFn;
import clojure.lang.IteratorSeq;
import java.util.ArrayList;
import java.util.List;

public class Api {
    public static Flow compileFlow(String name, List<Tap> taps, List<Object> gens) {
        List<Object> args = toCompileFlowArgs(name, taps, gens);
        return (Flow) getFn("compile-flow").applyTo(IteratorSeq.create(args.iterator()));
    }
    
    public static Flow compileFlow(List<Tap> taps, List<Object> gens) {
        return compileFlow(null, taps, gens);
    }
    
    public static void execute(String name, List<Tap> taps, List<Object> gens) {
        List<Object> args = toCompileFlowArgs(name, taps, gens);
        getFn("?-").applyTo(IteratorSeq.create(args.iterator()));
    }
    
    public static void execute(List<Tap> taps, List<Object> gens) {
        execute(null, taps, gens);
    }
    
    public static Object union(Object... gens) {
        return union(Util.toList(gens));
    }
    
    public static Object union(List<Object> gens) {
        return getFn("union").applyTo(IteratorSeq.create(gens.iterator()));
    }
    
    public static Object combine(Object... gens) {
        return combine(Util.toList(gens));
    }
    
    public static Object combine(List<Object> gens) {
        return getFn("combine").applyTo(IteratorSeq.create(gens.iterator()));
    }
    
    public static int numOutFields(Object gen) {
        return ((Number) getFn("num-out-fields").invoke(gen)).intValue();
    }
    
    public static Fields getOutFields(Object gen) {
        return new Fields((List<Object>)getFn("get-out-fields").invoke(gen));        
    }
    
    public static Object selectFields(Object gen, Fields fields) {
        return getFn("select-fields").invoke(gen, fields);
    }
    
    public static Object nameVars(Object gen, Fields vars) {
        return getFn("name-vars").invoke(gen, vars);        
    }
    
    private static IFn getFn(String name) {
        return Util.bootSimpleFn("cascalog.api", name);
    }
    
    private static List<Object> toCompileFlowArgs(String name, List<Tap> taps, List<Object> gens) {
        if(taps.size()!=gens.size()) {
            throw new IllegalArgumentException("Must have same number of taps and generators");
        }
        List<Object> args = new ArrayList<Object>();
        if(name!=null) args.add(name);
        for(int i=0; i<taps.size(); i++) {
            args.add(taps.get(i));
            args.add(gens.get(i));
        }
        return args;
    }
}
