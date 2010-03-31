package cascalog;

import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;

public class ClojureFilter extends BaseOperation implements Filter {
  private Object[] fn_spec;
  private IFn fn;
  
  public ClojureFilter(Object[] fn_spec) {
    this.fn_spec = fn_spec;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.fn = Util.bootFn(fn_spec);
  }

  public boolean isRemove(FlowProcess flow_process, FilterCall filter_call) {
    ISeq fn_args_seq = Util.coerceFromTuple(filter_call.getArguments().getTuple());
    try {
      return !Util.truthy(this.fn.applyTo(fn_args_seq));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
