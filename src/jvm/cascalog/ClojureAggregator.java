/*
    Copyright 2010 Nathan Marz
 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package cascalog;

import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Fields;
import clojure.lang.RT;
import clojure.lang.ISeq;
import java.util.Collection;

public class ClojureAggregator extends ClojureCascadingBase
                               implements Aggregator {
    
  public ClojureAggregator(Fields out_fields, Object[] fn_spec, boolean stateful) {
    super(out_fields, fn_spec, stateful);
  }
  
  public void start(FlowProcess flow_process, AggregatorCall ag_call) {
    ag_call.setContext(invokeFunction());
  }

  public void aggregate(FlowProcess flow_process, AggregatorCall ag_call) {
    ISeq fn_args_seq = Util.coerceFromTuple(ag_call.getArguments().getTuple());
    ag_call.setContext(
      applyFunction(RT.cons(ag_call.getContext(), fn_args_seq)));
  }

  public void complete(FlowProcess flow_process, AggregatorCall ag_call) {
    Collection coll = (Collection) invokeFunction(ag_call.getContext());
    
    TupleEntryCollector collector = ag_call.getOutputCollector();

    if (coll != null) {
        for(Object o: coll) {
            collector.add(Util.coerceToTuple(o));
        }
    }
  }
}
