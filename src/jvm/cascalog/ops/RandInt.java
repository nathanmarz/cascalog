package cascalog.ops;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import java.util.Random;
import cascalog.CascalogFunction;

public class RandInt extends CascalogFunction {
  long _seed;
  Random _rand;
  Integer _max;

  public RandInt() {
    _seed = new Random().nextLong();
    _max = null;
  }

  public RandInt(int max) {
    this();
    _max = max;
  }

  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    _rand = new Random(_seed + ((HadoopFlowProcess) flowProcess).getCurrentSliceNum());
  }

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
      int rand;
      if(_max==null) rand = _rand.nextInt();
      else rand = _rand.nextInt(_max);
      fn_call.getOutputCollector().add(new Tuple(rand));
  }
}
