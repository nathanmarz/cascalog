package cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import clojure.lang.IFn;
import clojure.lang.RT;
import java.util.List;

public class ClojureParallelAgg implements ParallelAgg {
    CombinerSpec _spec;
    IFn _initFn;
    IFn _combinerFn;
    
    public ClojureParallelAgg(CombinerSpec spec) {
        _spec = spec;
    }
    
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        _initFn = Util.bootFn(_spec.init_spec);
        _combinerFn = Util.bootFn(_spec.combiner_spec);
    }

    @Override
    public List<Object> init(List<Object> input) {
        return Util.coerceToList(_initFn.applyTo(RT.seq(input)));
    }

    @Override
    public List<Object> combine(List<Object> val1, List<Object> val2) {
        return Util.coerceToList(_combinerFn.applyTo(Util.cat(RT.seq(val1), RT.seq(val2))));
    }
    
}
