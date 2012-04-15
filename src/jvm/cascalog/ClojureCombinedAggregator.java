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

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import clojure.lang.ISeq;
import java.util.List;

public class ClojureCombinedAggregator extends BaseOperation<Object> implements Aggregator<Object> {
    private ParallelAgg _agg;

    public ClojureCombinedAggregator(Fields outfields, ParallelAgg agg) {
        super(outfields);
        _agg = agg;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Object> opCall) {
        _agg.prepare(flowProcess, opCall);
    }

    public void start(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        aggCall.setContext(null);
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        try {
            List<Object> args = Util.tupleToList(aggCall.getArguments());
            List<Object> currContext = (List<Object>) aggCall.getContext();
            if (currContext == null) {
                aggCall.setContext(args);
            } else {
                aggCall.setContext(_agg.combine(currContext, args));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void complete(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        try {
            aggCall.getOutputCollector().add(Util.coerceToTuple(aggCall.getContext()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
