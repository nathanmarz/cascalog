
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

import java.util.Collection;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascalog.ClojureCascadingBase;
import cascalog.Util;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

public class ClojureAggregator extends ClojureCascadingBase implements Aggregator {

  public ClojureAggregator(Fields outputFields, IFn fn) {
    super(outputFields, fn);
  }

  public void start(FlowProcess flow_process, AggregatorCall ag_call) {
    ag_call.setContext(invokeFunction());
  }

  public void aggregate(FlowProcess flow_process, AggregatorCall ag_call) {
    ISeq fn_args_seq = Util.coerceFromTuple(ag_call.getArguments().getTuple());
    ag_call.setContext(applyFunction(RT.cons(ag_call.getContext(), fn_args_seq)));
  }

  public void complete(FlowProcess flow_process, AggregatorCall ag_call) {
    Collection coll = (Collection) invokeFunction(ag_call.getContext());

    TupleEntryCollector collector = ag_call.getOutputCollector();

    if (coll != null) {
      for (Object o : coll) {
        collector.add(Util.coerceToTuple(o));
      }
    }
  }
}