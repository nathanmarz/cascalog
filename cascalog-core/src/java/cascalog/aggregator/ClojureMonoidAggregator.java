/*
    Copyright 2010 Nathan Marz

    Project and contact information: http://www.cascalog.org/

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package cascalog.aggregator;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascalog.Util;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

public class ClojureMonoidAggregator extends BaseOperation<Tuple> implements Aggregator<Tuple> {
  private final CombinerSpec combinerSpec;
  private transient IFn prepareFn;
  private transient IFn combineFn;
  private transient IFn presentFn;

  public ClojureMonoidAggregator(Fields fields, CombinerSpec combinerSpec) {
    super(fields);
    this.combinerSpec = combinerSpec;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    prepareFn = combinerSpec.getPrepareFn();
    combineFn = combinerSpec.getCombineFn();
    presentFn = combinerSpec.getPresentFn();
  }

  public void start(FlowProcess fp, AggregatorCall<Tuple> call) {
    call.setContext(null);
  }

  public void aggregate(FlowProcess fp, AggregatorCall<Tuple> call) {
    ISeq fnArgs = RT.seq(Util.tupleToList(call.getArguments()));
    if (null != prepareFn) {
      fnArgs = RT.seq(Util.coerceToList(prepareFn.applyTo(fnArgs)));
    }
    Tuple context = call.getContext();

    if (null == context) {
      Tuple newContext = Tuple.size(1);
      newContext.set(0, fnArgs);
      call.setContext(newContext);
    } else {
      ISeq acc = (ISeq) context.getObject(0);
      ISeq ret = RT.seq(Util.coerceToList(combineFn.applyTo(Util.cat(acc, fnArgs))));
      context.set(0, ret);
    }
  }

  public void complete(FlowProcess flowProcess, AggregatorCall<Tuple> call) {
    Tuple context = call.getContext();

    if (null == context) {
      throw new RuntimeException("ClojureMonoidAggregator completed with any aggregate calls");
    } else {
      ISeq finalValue = (ISeq) context.getObject(0);
      call.setContext(null);
      if (null != presentFn) {
        call.getOutputCollector().add(
            Util.coerceToTuple(presentFn.applyTo(finalValue)));
      } else {
        call.getOutputCollector().add(
            Util.coerceToTuple(finalValue));
      }
    }
  }
}
