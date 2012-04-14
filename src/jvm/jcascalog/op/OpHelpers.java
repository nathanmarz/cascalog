package jcascalog.op;

import cascalog.Util;
import clojure.lang.ArraySeq;
import clojure.lang.IFn;
import clojure.lang.IteratorSeq;
import clojure.lang.Keyword;
import java.util.ArrayList;
import java.util.List;

public class OpHelpers {
    public static Object negate(Object op) {
        return getFn("negate").invoke(op);
    }
    
    public static Object all(Object... ops) {
        return getFn("all").applyTo(ArraySeq.create(ops));
    }
    
    public static Object any(Object... ops) {
        return getFn("any").applyTo(ArraySeq.create(ops));        
    }
    
    public static Object comp(Object... ops) {
        return getFn("comp").applyTo(ArraySeq.create(ops));                
    }
    
    public static Object juxt(Object... ops) {
        return getFn("juxt").applyTo(ArraySeq.create(ops));                        
    }
    
    public static Object each(Object... ops) {
        return getFn("each").applyTo(ArraySeq.create(ops));                                
    }
    
    public static Object partial(Object op, Object... args) {
        List<Object> all = new ArrayList<Object>();
        all.add(op);
        for(Object o: args) {
            all.add(o);
        }
        return getFn("partial").applyTo(IteratorSeq.create(all.iterator()));
    }
    
    public static Object firstN(Object gen, int n) {
        return firstN(gen, n, new FirstNArgs());        
    }

    public static Object firstN(Object gen, int n, FirstNArgs args) {
        List<Object> all = new ArrayList<Object>();
        all.add(gen);
        all.add(n);
        all.add(Keyword.intern(":sort"));
        all.add(args.sortParam);
        all.add(Keyword.intern("reverse"));
        all.add(args.reverse);
        return getFn("first-n").applyTo(IteratorSeq.create(all.iterator()));
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
    
    private static IFn getFn(String name) {
        return Util.bootSimpleFn("cascalog.ops", name);
    }
}
