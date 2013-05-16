package cascalog;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class MonoidAgg
 */
public class ClojureMonoidFunctor extends ClojureCascadingBase implements AggregateBy.Functor {
    transient boolean inited  = false;

    public ClojureMonoidFunctor(Fields fields, Object[] fn_spec, boolean stateful) {
        super(fields, fn_spec, stateful);
    }

    @Override
    public Fields getDeclaredFields() {
        return super.getFieldDeclaration();
    }

    @Override
    public Tuple aggregate(FlowProcess flowProcess, TupleEntry tupleEntry, Tuple result) {
        if(!inited) {
            prepare(flowProcess, null);
            inited = true;
        }

        final Tuple args = tupleEntry.getTuple();
        if(result == null) {
            return args;
        } else {
            return Util.coerceToTuple(invokeFunction(Util.coerceFromTuple(args)));
        }
    }

    @Override
    public Tuple complete(FlowProcess flowProcess, Tuple result) {
        if(inited) {
            cleanup(flowProcess, null);
            inited = false;
        }
        return result;
    }
}
