package jcascalog;

import cascading.flow.Flow;
import cascading.tap.Tap;
import cascalog.Util;
import clojure.lang.ArraySeq;
import clojure.lang.IFn;
import clojure.lang.IteratorSeq;
import clojure.lang.Keyword;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Api {
    public static Flow compileFlow(String name, List<Tap> taps, List<Object> gens) {
        List<Object> args = toCompileFlowArgs(name, taps, gens);
        return (Flow) getApiFn("compile-flow").applyTo(IteratorSeq.create(args.iterator()));
    }
    
    public static Flow compileFlow(List<Tap> taps, List<Object> gens) {
        return compileFlow(null, taps, gens);
    }

    public static Flow compileFlow(String name, Tap tap, Object gen) {
        return compileFlow(name, Arrays.asList(tap), Arrays.asList(gen));
    } 
    
    public static Flow compileFlow(Tap tap, Object gen) {
        return compileFlow(Arrays.asList(tap), Arrays.asList(gen));
    } 
    
    public static void execute(String name, List<Tap> taps, List<Object> gens) {
        List<Object> args = toCompileFlowArgs(name, taps, gens);
        getApiFn("?-").applyTo(IteratorSeq.create(args.iterator()));
    }
    
    public static void execute(String name, Tap tap, Object gen) {
        execute(name, Arrays.asList(tap), Arrays.asList(gen));
    } 
    
    public static void execute(Tap tap, Object gen) {
        execute(Arrays.asList(tap), Arrays.asList(gen));
    }
    
    public static void execute(List<Tap> taps, List<Object> gens) {
        execute(null, taps, gens);
    }
    
    public static Object union(Object... gens) {
        return union(Util.toList(gens));
    }
    
    public static Object union(List<Object> gens) {
        return getApiFn("union").applyTo(IteratorSeq.create(gens.iterator()));
    }
    
    public static Object combine(Object... gens) {
        return combine(Util.toList(gens));
    }
    
    public static Object combine(List<Object> gens) {
        return getApiFn("combine").applyTo(IteratorSeq.create(gens.iterator()));
    }
    
    public static int numOutFields(Object gen) {
        return ((Number) getApiFn("num-out-fields").invoke(gen)).intValue();
    }
    
    public static Fields getOutFields(Object gen) {
        return new Fields((List<Object>) getApiFn("get-out-fields").invoke(gen));        
    }
    
    public static Object selectFields(Object gen, Fields fields) {
        return getApiFn("select-fields").invoke(gen, fields);
    }
    
    public static Object nameVars(Object gen, Fields vars) {
        return getApiFn("name-vars").invoke(gen, vars);        
    }
    
    public static String genNullableVar() {
        return (String) Util.bootSimpleFn("cascalog.vars", "gen-nullable-var").invoke();
    }
    
    public static Fields genNullableVars(int amt) {
        List<String> vars = (List<String>) Util.bootSimpleFn("cascalog.vars", "gen-nullable-vars").invoke(amt);
        return new Fields(vars);
    }
    
    public static void setApplicationConf(Map conf) {
        Util.bootSimpleFn("cascalog.conf", "set-job-conf!").invoke(conf);
    }
    
    public static Object negate(Object op) {
        return getOpFn("negate").invoke(op);
    }
    
    public static Object all(Object... ops) {
        return getOpFn("all").applyTo(ArraySeq.create(ops));
    }
    
    public static Object any(Object... ops) {
        return getOpFn("any").applyTo(ArraySeq.create(ops));        
    }
    
    public static Object comp(Object... ops) {
        return getOpFn("comp").applyTo(ArraySeq.create(ops));                
    }
    
    public static Object juxt(Object... ops) {
        return getOpFn("juxt").applyTo(ArraySeq.create(ops));                        
    }
    
    public static Object each(Object... ops) {
        return getOpFn("each").applyTo(ArraySeq.create(ops));                                
    }
    
    public static Object partial(Object op, Object... args) {
        List<Object> all = new ArrayList<Object>();
        all.add(op);
        for(Object o: args) {
            all.add(o);
        }
        return getOpFn("partial").applyTo(IteratorSeq.create(all.iterator()));
    }
    
    public static Object firstN(Object gen, int n) {
        return firstN(gen, n, new FirstNArgs());        
    }

    public static Object firstN(Object gen, int n, FirstNArgs args) {
        List<Object> all = new ArrayList<Object>();
        all.add(gen);
        all.add(n);
        all.add(Keyword.intern("sort"));
        all.add(args.sortParam);
        all.add(Keyword.intern("reverse"));
        all.add(args.reverse);
        return getOpFn("first-n").applyTo(IteratorSeq.create(all.iterator()));
    }
    
    public static class FirstNArgs {
        private Object sortParam = null;
        private boolean reverse = false;
        
        public FirstNArgs sort(String field) {
            sortParam = field;
            return this;
        }
        
        public FirstNArgs sort(List<String> fields) {
            sortParam = fields;
            return this;
        }
        
        public FirstNArgs reverse(boolean reverse) {
            this.reverse = reverse;
            return this;
        }
    }
    
    private static IFn getApiFn(String name) {
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

    private static IFn getOpFn(String name) {
        return Util.bootSimpleFn("cascalog.ops", name);
    }
}
