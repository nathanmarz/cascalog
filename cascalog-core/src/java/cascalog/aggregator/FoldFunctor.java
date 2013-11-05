package cascalog.aggregator;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Straight-up port of Scalding's FoldFunctor.
 */
public abstract class FoldFunctor<T> implements AggregateBy.Functor {
  protected final Fields fields;
  protected Tuple nextContext = null;

  public FoldFunctor(Fields fields) {
    this.fields = fields;
  }

  public abstract T prepare(TupleEntry args);
  public abstract T fold(T acc, TupleEntry newArgs);
  public abstract Tuple present(T finalValue);

  @Override
  public Fields getDeclaredFields() {
    return fields;
  }

  @Override
  public Tuple aggregate(FlowProcess flowProcess, TupleEntry args, Tuple context) {
    T nextContextObj;
    if (null == context) {
      nextContext = Tuple.size(1);
      nextContextObj = prepare(args);
    } else {
      T oldValue = (T) context.getObject(0);
      nextContext = context;
      nextContextObj = fold(oldValue, args);
    }
    nextContext.set(0, nextContextObj);
    return nextContext;
  }

  @Override
  public Tuple complete(FlowProcess flowProcess, Tuple context) {
    if (null == context) {
      throw new RuntimeException("ClojureMonoidFunctor completed with any aggregate calls");
    } else {
      T result = (T) context.getObject(0);
      context.set(0, null);
      return present(result);
    }
  }
}
