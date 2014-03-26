package cascalog.aggregator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.Util;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

public class ClojureMonoidFunctor extends FoldFunctor<ISeq> {
  final CombinerSpec combinerSpec;
  transient IFn prepareFn;
  transient IFn combineFn;
  transient boolean isPrepared = false;

  public ClojureMonoidFunctor(Fields fields, CombinerSpec combinerSpec) {
    super(fields);
    this.combinerSpec = combinerSpec;
  }

  @Override
  public ISeq prepare(TupleEntry args) {
    if (!isPrepared) {
      prepareFn = combinerSpec.getPrepareFn();
      combineFn = combinerSpec.getCombineFn();
      isPrepared = true;
    }
    return applyPrepareFn(args);
  }

  public ISeq applyPrepareFn(TupleEntry args) {
    ISeq input = RT.seq(Util.tupleToList(args));
    if (null != prepareFn) {
      return RT.seq(Util.coerceToList(prepareFn.applyTo(input)));
    } else {
      return input;
    }
  }

  @Override
  public ISeq fold(ISeq acc, TupleEntry newArgs) {
    return RT.seq(Util.coerceToList(
        combineFn.applyTo(
         Util.cat(acc, applyPrepareFn(newArgs)))));
  }

  @Override
  public Tuple present(ISeq finalValue) {
    return Util.coerceToTuple(finalValue);
  }
}
