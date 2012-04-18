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
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

import java.util.Arrays;
import java.util.List;

public class ClojureBufferCombiner extends ClojureCombinerBase {

    private CombinerSpec spec;

    public ClojureBufferCombiner(Fields groupFields, Fields sortFields, Fields args,
        Fields outFields, CombinerSpec spec) {
        super(groupFields, true, sortFields, Arrays.asList(args), outFields,
                Arrays.asList((ParallelAgg) new ClojureParallelAgg(spec)),
                "cascalog.combiner.buffer.size", 200);
        this.spec = spec;
    }

    private IFn extract_fn = null;

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        super.prepare(flowProcess, operationCall);
        extract_fn = Util.bootFn(spec.extractor_spec);
    }

    @Override
    protected void write(Tuple group, List<Object> vals, OperationCall opCall) {
        TupleEntryCollector output = ((FunctionCall) opCall).getOutputCollector();

        if (vals.size() != 1) {
            throw new RuntimeException(
                "Should only have one object in buffer combiner before extraction " + vals.size()
                + ":" + vals.toString());
        }
        Object val = vals.get(0);
        try {
            ISeq result_seq = RT.seq(extract_fn.invoke(val));
            while (result_seq != null) {
                Tuple t = Util.coerceToTuple(result_seq.first());
                Tuple emit = new Tuple(group);
                emit.addAll(t);
                output.add(emit);
                result_seq = result_seq.next();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}