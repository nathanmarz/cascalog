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

import cascading.operation.BaseOperation;
import cascading.operation.Aggregator;
import cascading.operation.OperationCall;
import cascading.operation.AggregatorCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import java.util.Collection;

public class ClojureCombinedAggregator extends BaseOperation<Object>
                               implements Aggregator<Object> {
  private Object[] combine_spec;
  private IFn combine_fn;

  public ClojureCombinedAggregator(String out_field, Object[] combine_spec) {
    super(new Fields(out_field));
    this.combine_spec = combine_spec;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall<Object> op_call) {
    this.combine_fn = Util.bootFn(combine_spec);
  }

  public void start(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
    ag_call.setContext(null);
  }

  public void aggregate(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
    try {
      Object o = ag_call.getArguments().get(0);      
      Object currContext = ag_call.getContext();
      if(currContext==null) {
          ag_call.setContext(o);
      } else {
          ag_call.setContext(this.combine_fn.invoke(currContext, o));
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
