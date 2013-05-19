package cascalog;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import clojure.lang.RT;

/**
 * Class MonoidAgg
 */
public class ClojureMonoidFunctor extends ClojureCascadingBase implements AggregateBy.Functor {
    transient boolean inited  = false;

    public ClojureMonoidFunctor(Fields fields, byte[] fn_spec, boolean stateful) {
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
            Object arg1 = result.getObject(0);
            Object arg2 = args.getObject(0);
            return Util.coerceToTuple(applyFunction(RT.list(arg1, arg2)));
        }
    }

    @Override
    public Tuple complete(FlowProcess flowProcess, Tuple result) {
        return result;
    }
}
