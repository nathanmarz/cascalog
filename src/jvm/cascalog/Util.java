/*
    Copyright 2010 Nathan Marz
 
    Project and contact information: http://www.cascalog.org/ 

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package cascalog;

import clojure.lang.Compiler;
import clojure.lang.RT;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.ArraySeq;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Util {

  public static ISeq cat(ISeq s1, ISeq s2) {
      if(s1==null || RT.seq(s1)==null) return s2;
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
    if(o instanceof Collection) {
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
    if(obj instanceof Collection) {
      Object[] arr = ((Collection)obj).toArray();
      return new Tuple(arr);      
    } else {
      return new Tuple(obj);
    }
  }

  public static List seqToList(ISeq s) {
      s = RT.seq(s);
      List<Object> ret = new ArrayList<Object>();
      while(s!=null) {
          ret.add(s.first());
          s = s.next();
      }
      return ret;
  }
  
  public static boolean truthy(Object obj) {
    return ((obj != null) && (!Boolean.FALSE.equals(obj)));
  }
}
