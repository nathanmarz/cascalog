package cascalog;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import java.util.Collection;

public class ClojureMap extends BaseOperation implements Function {
  private Object[] fn_spec;
  private IFn fn;
  
  public ClojureMap(Fields out_fields, Object[] fn_spec) {
    super(out_fields);
    this.fn_spec = fn_spec;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.fn = Util.bootFn(fn_spec);
  }

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
    ISeq fn_args_seq = Util.coerceFromTuple(fn_call.getArguments().getTuple());
    try {
      Object res = this.fn.applyTo(fn_args_seq);
      fn_call.getOutputCollector().add(Util.coerceToTuple(res));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
