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

package cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;

public class ClojureCombinedAggregator extends BaseOperation<Object> implements Aggregator<Object> {
    private Object[] combine_spec;
    private IFn combine_fn;

    public ClojureCombinedAggregator(Fields outfields, Object[] combine_spec) {
        super(outfields);
        this.combine_spec = combine_spec;
    }

    @Override
    public void prepare(FlowProcess flow_process, OperationCall<Object> op_call) {
        this.combine_fn = Util.bootFn(combine_spec);
    }

    public void start(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
        ag_call.setContext(null);
    }

    public void aggregate(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
        try {
            ISeq args = Util.coerceFromTuple(ag_call.getArguments());
            ISeq currContext = (ISeq) ag_call.getContext();
            if (currContext == null) {
                ag_call.setContext(args);
            } else {
                ag_call.setContext(Util
                    .coerceToSeq(this.combine_fn.applyTo(Util.cat(currContext, args))));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void complete(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
        try {
            ag_call.getOutputCollector().add(Util.coerceToTuple(ag_call.getContext()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
