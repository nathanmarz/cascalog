package cascalog.aggregator;

import cascading.operation.Aggregator;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

public class ClojureAggregateBy extends AggregateBy {
  public ClojureAggregateBy(Fields argumentFields, Functor functor, Aggregator aggregator) {
    super(argumentFields, functor, aggregator);
  }
}
