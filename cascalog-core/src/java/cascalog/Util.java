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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.MultiFn;
import clojure.lang.RT;
import clojure.lang.Var;

public class Util {
  static final Var require = RT.var("clojure.core", "require");
  static final Var symbol = RT.var("clojure.core", "symbol");

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
  public static synchronized void tryRequire(String ns_name) {
    try {
      require.invoke(symbol.invoke(ns_name));
    } catch (Exception e) {
      //if playing from the repl and defining functions, file won't exist
      Throwable rootCause = getRootCause(e);

      boolean fileNotFound = (rootCause instanceof FileNotFoundException);
      boolean nsFileMissing = e.getMessage().contains(ns_name + ".clj on classpath");

      if (!(fileNotFound && nsFileMissing)) { throw new RuntimeException(e); }
    }
  }

  public static synchronized Var getVar(String ns_name, String fn_name) {
    tryRequire(ns_name);
    return RT.var(ns_name, fn_name);
  }

  public static synchronized IFn bootSimpleFn(String ns_name, String fn_name) {
    return (IFn) getVar(ns_name, fn_name).deref();
  }

  public static synchronized MultiFn bootSimpleMultifn(String ns_name, String fn_name) {
    return (MultiFn) getVar(ns_name, fn_name).deref();
  }

  public static synchronized IFn deserializeFn(byte[] fnSpec) {
    return (IFn)  bootSimpleFn("cascalog.logic.fn", "deserialize").invoke(fnSpec);
  }

  public static synchronized byte[] serializeFn(IFn fn) {
    return (byte[]) bootSimpleFn("cascalog.logic.fn", "serialize").invoke(fn);
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

  public static void tupleIntoList(List<Object> ret, Tuple tuple) {
    for (Object aTuple : tuple) {
      ret.add(aTuple);
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
    Collections.addAll(ret, arr);
    return ret;
  }
}
