/*
    Copyright 2010 Nathan Marz
 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package cascalog;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import clojure.lang.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Util {
    static Var require = RT.var("clojure.core", "require");
    static Var symbol = RT.var("clojure.core", "symbol");
    static IFn serializeFn = bootSimpleFn("serializable.fn", "serialize");
    static IFn deserializeFn = bootSimpleFn("serializable.fn", "deserialize");

    public static ISeq cat(ISeq s1, ISeq s2) {
        if (s1 == null || RT.seq(s1) == null) { return s2; }
        return cat(s1.next(), s2).cons(s1.first());
    }

    public static Throwable getRootCause(Throwable e) {
        Throwable rootCause = e;
        Throwable nextCause = rootCause.getCause();

        while (nextCause != null) {
            rootCause = nextCause;
            nextCause = rootCause.getCause();
        }
        return rootCause;
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static void tryRequire(String ns_name) {
        try {
            require.invoke(symbol.invoke(ns_name));
        } catch (Exception e) {

            //if playing from the repl and defining functions, file won't exist
            Throwable rootCause = getRootCause(e);

            boolean fileNotFound = (rootCause instanceof FileNotFoundException);
            boolean nsFileMissing = e.getMessage().contains(ns_name + ".clj on classpath");

            if (!(fileNotFound && nsFileMissing))
                throw new RuntimeException(e);
        }
    }
    
    public static Var getVar(String ns_name, String fn_name) {
        tryRequire(ns_name);
        return RT.var(ns_name, fn_name);        
    }
    
    public static IFn bootSimpleFn(String ns_name, String fn_name) {
        return getVar(ns_name, fn_name);
    }

    public static IFn bootFn(Object[] fn_spec) {
        String ns_name = (String) fn_spec[0];
        String fn_name = (String) fn_spec[1];
        IFn simple_fn = bootSimpleFn(ns_name, fn_name);
        if (fn_spec.length == 2) {
            return simple_fn;
        } else {
            ISeq hof_args = ArraySeq.create(fn_spec).next().next();
            try {
                return (IFn) simple_fn.applyTo(hof_args);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static ISeq coerceToSeq(Object o) {
        if (o instanceof List) {
            return RT.seq(o);
        } else {
            return RT.list(o);
        }
    }

    public static List coerceToList(Object o) {
        if (o instanceof List) {
            return (List) o;
        } else {
            return Arrays.asList(o);
        }
    }    
    
    public static IteratorSeq coerceFromTuple(Tuple tuple) {
        return IteratorSeq.create(tuple.iterator());
    }

    public static IteratorSeq coerceFromTuple(TupleEntry tuple) {
        return coerceFromTuple(tuple.getTuple());
    }

    public static Tuple coerceToTuple(Object obj) {
        if (obj instanceof List) {
            Object[] arr = ((List) obj).toArray();
            return new Tuple(arr);
        } else {
            return new Tuple(obj);
        }
    }

    public static List seqToList(ISeq s) {
        s = RT.seq(s);
        List<Object> ret = new ArrayList<Object>();
        while (s != null) {
            ret.add(s.first());
            s = s.next();
        }
        return ret;
    }

    // TODO: convert to RT.booleanCast
    public static boolean truthy(Object obj) {
        return ((obj != null) && (!Boolean.FALSE.equals(obj)));
    }
    
    public static byte[] serializeFn(IFn fn) {
        return (byte[]) serializeFn.invoke(fn);
    }

    public static IFn deserializeFn(byte[] ser) {
        return (IFn) deserializeFn.invoke(ser);
    }

    public static void tupleIntoList(List<Object> ret, Tuple tuple) {
        Iterator it = tuple.iterator();
        while(it.hasNext()) {
            ret.add(it.next());
        }
    }
    
    public static List<Object> tupleToList(Tuple tuple) {
        List<Object> ret = new ArrayList<Object>();
        tupleIntoList(ret, tuple);
        return ret;
    }
    
    public static List<Object> tupleToList(TupleEntry tuple) {
        return tupleToList(tuple.getTuple());
    }
    
    public static List<Object> toList(Object[] arr) {
        List<Object> ret = new ArrayList<Object>();
        for(Object o: arr) {
            ret.add(o);
        }
        return ret;
    }
}
