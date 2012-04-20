package cascalog.ops;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import java.util.Random;
import cascalog.CascalogFunction;

public class RandLong extends CascalogFunction {
  long _seed;
  Random _rand;

  public RandLong() {
    _seed = new Random().nextLong();
  }

  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    _rand = new Random(_seed + ((HadoopFlowProcess) flowProcess).getCurrentSliceNum());
  }

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
      fn_call.getOutputCollector().add(new Tuple(_rand.nextLong()));
  }
}
