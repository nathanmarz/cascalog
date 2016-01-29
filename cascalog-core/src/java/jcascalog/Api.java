package jcascalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascalog.Util;
import clojure.lang.ArraySeq;
import clojure.lang.IFn;
import clojure.lang.IteratorSeq;
import clojure.lang.Keyword;

/**
 * This class is the core entry point to using JCascalog. Effectively it is a simple Java wrapper
 * around Clojure based Cascalog method calls.
 * <p/>
 * A crucial and important caveat to using this class
 * is that it is completely <strong>not safe</strong> to use across multiple threads. All of the
 * methods below are <em>static</em> and rely on <strong>hidden</strong> static state created
 * inside Clojure based calls. All of the publicly exposed methods of this class have been
 * made <em>synchronized</em> (and as such holding {@code Api.class} lock) to minimize this
 * problem. This is however <strong>not</strong> sufficient. The way Cascading {@link Flow}s
 * are build with JCascalog is through a <em>sequence of multiple calls</em> to this class.
 * Thus the actual operation is <strong>compound</strong>, and therefore the whole build flow logic must
 * be guarded with the same shared global lock (i.e. {@code synchronized(Api.class)}).
 * <p/>
 * Furthermore, care must be taken not to execute the actual Hadoop jobs with this shared lock
 * as this would defeat the purpose of running multiple jobs from a single JVM (presumably
 * this would be the reason to allow multi-threaded access in the first place).
 * The desired pattern thus is to 1) build the actual Cascading {@link Flow} using the shared lock
 * but 2) call {@link Flow#complete()} outside the _synchronized_ block to enable multi-threaded
 * execution of Hadoop jobs.
 *
 */

public class Api {
  public static synchronized Object hfsTextline(String path) {
    return getApiFn("hfs-textline").invoke(path);
  }

  public static synchronized Object hfsSeqfile(String path) {
    return getApiFn("hfs-seqfile").invoke(path);
  }

  public static synchronized Object StdoutTap() {
      return getApiFn("stdout").invoke();
  }

  public static synchronized Flow compileFlow(String name, List<Object> taps, List<Object> gens) {
    return compileFlow(flowDef(name, taps, gens));
  }

  public static synchronized Flow compileFlow(List<Object> taps, List<Object> gens) {
    return compileFlow(null, taps, gens);
  }

  public static synchronized Flow compileFlow(String name, Object tap, Object gen) {
    return compileFlow(name, Arrays.asList(tap), Arrays.asList(gen));
  }

  public static synchronized Flow compileFlow(Object tap, Object gen) {
    return compileFlow(Arrays.asList(tap), Arrays.asList(gen));
  }

  public static synchronized Flow compileFlow(FlowDef flowDef) {
      return (Flow) getApiFn("compile-hadoop").invoke(flowDef);
  }

  public static synchronized FlowDef flowDef(String name, List<Object> taps, List<Object> gens) {
    List<Object> args = toCompileFlowArgs(name, taps, gens);
    return (FlowDef) getApiFn("jflow-def").applyTo(IteratorSeq.create(args.iterator()));
  }

  public static synchronized void execute(String name, List<Object> taps, List<Object> gens) {
    List<Object> args = toCompileFlowArgs(name, taps, gens);
    getApiFn("?-").applyTo(IteratorSeq.create(args.iterator()));
  }

  public static synchronized void execute(String name, Object tap, Object gen) {
    execute(name, Arrays.asList(tap), Arrays.asList(gen));
  }

  public static synchronized void execute(Object tap, Object gen) {
    execute(Arrays.asList(tap), Arrays.asList(gen));
  }

  public static synchronized void execute(List<Object> taps, List<Object> gens) {
    execute(null, taps, gens);
  }

  public static synchronized void setCascadingPlatform() {
      getApiFn("set-cascading-platform!").invoke();
  }

  public static synchronized void setInMemoryPlatform() {
      getApiFn("set-in-memory-platform!").invoke();
  }

  public static synchronized Object union(Object... gens) {
    return union(Util.toList(gens));
  }

  public static synchronized Object union(List<Object> gens) {
    return getApiFn("union").applyTo(IteratorSeq.create(gens.iterator()));
  }

  public static synchronized Object combine(Object... gens) {
    return combine(Util.toList(gens));
  }

  public static synchronized Object combine(List<Object> gens) {
    return getApiFn("combine").applyTo(IteratorSeq.create(gens.iterator()));
  }

  public static synchronized int numOutFields(Object gen) {
    return ((Number) getApiFn("num-out-fields").invoke(gen)).intValue();
  }

  public static synchronized Fields getOutFields(Object gen) {
    return new Fields((List<Object>) getApiFn("get-out-fields").invoke(gen));
  }

  public static synchronized Object selectFields(Object gen, Fields fields) {
    return getApiFn("select-fields").invoke(gen, fields);
  }

  public static synchronized Object nameVars(Object gen, Fields vars) {
    return getApiFn("name-vars").invoke(gen, vars);
  }

  public static synchronized String genNullableVar() {
    return (String) Util.bootSimpleFn("cascalog.logic.vars", "gen-nullable-var").invoke();
  }

  public static synchronized Fields genNullableVars(int amt) {
    List<String> vars =
        (List<String>) Util.bootSimpleFn("cascalog.logic.vars", "gen-nullable-vars").invoke(amt);
    return new Fields(vars);
  }

  public static synchronized void setApplicationConf(Map conf) {
    Util.bootSimpleFn("cascalog.cascading.conf", "set-job-conf!").invoke(conf);
  }

  public static synchronized Object negate(Object op) {
    return getOpFn("negate").invoke(op);
  }

  public static synchronized Object all(Object... ops) {
    return getOpFn("all").applyTo(ArraySeq.create(ops));
  }

  public static synchronized Object any(Object... ops) {
    return getOpFn("any").applyTo(ArraySeq.create(ops));
  }

  public static synchronized Object comp(Object... ops) {
    return getOpFn("comp").applyTo(ArraySeq.create(ops));
  }

  public static synchronized Object juxt(Object... ops) {
    return getOpFn("juxt").applyTo(ArraySeq.create(ops));
  }

  public static synchronized Object each(Object op) {
    return getOpFn("each").invoke(op);
  }

  public static synchronized Object partial(Object op, Object... args) {
    List<Object> all = new ArrayList<Object>();
    all.add(op);
    for (Object o : args) {
      all.add(o);
    }
    return getOpFn("partial").applyTo(IteratorSeq.create(all.iterator()));
  }

  public static synchronized Object firstN(Object gen, int n) {
    return firstN(gen, n, new FirstNArgs());
  }

  public static synchronized Object firstN(Object gen, int n, FirstNArgs args) {
    List<Object> all = new ArrayList<Object>();
    all.add(gen);
    all.add(n);
    all.add(Keyword.intern("sort"));
    all.add(args.sortParam);
    all.add(Keyword.intern("reverse"));
    all.add(args.reverse);
    return getOpFn("first-n").applyTo(IteratorSeq.create(all.iterator()));
  }

  public static synchronized Object fixedSample(Object gen, int amt) {
    return getOpFn("fixed-sample").invoke(gen, amt);
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

  private static List<Object> toCompileFlowArgs(String name, List<Object> taps, List<Object> gens) {
    if (taps.size() != gens.size()) {
      throw new IllegalArgumentException("Must have same number of taps and generators");
    }
    List<Object> args = new ArrayList<Object>();
    if (name != null) { args.add(name); }
    for (int i = 0; i < taps.size(); i++) {
      args.add(taps.get(i));
      args.add(gens.get(i));
    }
    return args;
  }

  private static IFn getOpFn(String name) {
    return Util.bootSimpleFn("cascalog.logic.ops", name);
  }
}
