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
import clojure.lang.Compiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Util {

    public static ISeq cat(ISeq s1, ISeq s2) {
        if (s1 == null || RT.seq(s1) == null) { return s2; }
        return cat(s1.next(), s2).cons(s1.first());
    }

    public static IFn bootSimpleFn(String ns_name, String fn_name) {
        try {
            Compiler.eval(RT.readString("(require '" + ns_name + ")"));
        } catch (Exception e) {
            //if playing from the repl and defining functions, file won't exist
        }
        return (IFn) RT.var(ns_name, fn_name).deref();
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
        if (o instanceof Collection) {
            return RT.seq(o);
        } else {
            return RT.list(o);
        }
    }

    public static IteratorSeq coerceFromTuple(Tuple tuple) {
        return IteratorSeq.create(tuple.iterator());
    }

    public static IteratorSeq coerceFromTuple(TupleEntry tuple) {
        return coerceFromTuple(tuple.getTuple());
    }

    public static Tuple coerceToTuple(Object obj) {
        if (obj instanceof Collection) {
            Object[] arr = ((Collection) obj).toArray();
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

    public static boolean truthy(Object obj) {
        return ((obj != null) && (!Boolean.FALSE.equals(obj)));
    }
}
