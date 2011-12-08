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

public class ClojureParallelAggregator extends BaseOperation<Object> implements Aggregator<Object> {
    private Object[] init_spec;
    private Object[] combine_spec;
    private IFn init_fn;
    private IFn combine_fn;
    private int args;

    public ClojureParallelAggregator(Fields outfields, Object[] init_spec, Object[] combine_spec,
        int args) {
        super(outfields);
        this.init_spec = init_spec;
        this.combine_spec = combine_spec;
        this.args = args;
    }

    public void prepare(FlowProcess flow_process, OperationCall<Object> op_call) {
        this.init_fn = Util.bootFn(init_spec);
        this.combine_fn = Util.bootFn(combine_spec);
    }

    public void start(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
        ag_call.setContext(null);
    }

    public void aggregate(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
        try {
            ISeq fn_args_seq = Util.coerceFromTuple(ag_call.getArguments().getTuple());
            Object o;
            if (this.args > 0) { o = this.init_fn.applyTo(fn_args_seq); } else {
                o = this.init_fn.invoke();
            }

            ISeq oseq = Util.coerceToSeq(o);

            ISeq currContext = (ISeq) ag_call.getContext();
            if (currContext == null) {
                ag_call.setContext(oseq);
            } else {
                ag_call.setContext(Util
                    .coerceToSeq(this.combine_fn.applyTo(Util.cat(currContext, oseq))));
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
