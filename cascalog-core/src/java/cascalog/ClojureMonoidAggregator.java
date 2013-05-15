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
import cascading.tuple.Fields;
import clojure.lang.ISeq;
import clojure.lang.RT;

@SuppressWarnings("unchecked")
public class ClojureMonoidAggregator extends ClojureCascadingBase implements Aggregator {
    public ClojureMonoidAggregator(Fields out_fields, Object[] fn_spec, boolean stateful) {
        super(out_fields, fn_spec, stateful);
    }

    public void start(FlowProcess flowProcess, AggregatorCall aggCall) {
        aggCall.setContext(null);
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall aggCall) {
        ISeq argSeq = Util.coerceFromTuple(aggCall.getArguments().getTuple());
        final Object context = aggCall.getContext();
        if(context == null) {
            aggCall.setContext(argSeq);
        } else {
            aggCall.setContext(applyFunction(RT.cons(aggCall.getContext(), argSeq)));
        }
    }

    public void complete(FlowProcess flowProcess, AggregatorCall aggCall) {
        try {
            final Object context = aggCall.getContext();
            if(context != null) {
                aggCall.getOutputCollector().add(Util.coerceToTuple(context));
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
