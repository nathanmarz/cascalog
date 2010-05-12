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
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

public abstract class ClojureCombinerBase extends BaseOperation implements Function {
    private List<CombinerSpec> specs;
    private Fields groupFields;
    private Fields sortFields;
    private List<Fields> argFields;
    private List<IFn> init_fns = null;
    private List<IFn> combiner_fns = null;
    private boolean includeSort;
    private int cacheSize;

    private static Fields appendFields(Fields start, Fields... rest) {
        for(Fields f: rest) {
            if(f!=null) {
                start = start.append(f);
            }
         }
         return start;
    }

    public ClojureCombinerBase(Fields groupFields, boolean includeSort, Fields sortFields, List<Fields> argFields, Fields outFields, List<CombinerSpec> agg_specs, int cacheSize) {
        super(appendFields(groupFields, sortFields, outFields));
        if(argFields.size()!=agg_specs.size())
            throw new IllegalArgumentException("All lists to ClojureCombiner must be same length");
        this.specs = new ArrayList<CombinerSpec>(agg_specs);
        this.groupFields = groupFields;
        this.sortFields = sortFields;
        this.includeSort = includeSort;
        this.argFields = new ArrayList<Fields>(argFields);
        this.cacheSize = cacheSize;
    }

    LinkedHashMap<Tuple, List<Object>> combined;

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        combined = new LinkedHashMap<Tuple, List<Object>>(1000, (float) 0.75, true);
        init_fns = new ArrayList<IFn>();
        combiner_fns = new ArrayList<IFn>();
        for(CombinerSpec cs: this.specs) {
            init_fns.add(Util.bootFn(cs.init_spec));
            combiner_fns.add(Util.bootFn(cs.combiner_spec));
        }
    }

    public void operate(FlowProcess fp, FunctionCall fc) {
        Tuple group = fc.getArguments().selectTuple(groupFields);
        Tuple sortArgs = null;
        if(includeSort) {
            if(sortFields!=null)
                sortArgs = fc.getArguments().selectTuple(sortFields);
            else
                sortArgs = new Tuple();
        }
        List<Object> vals = combined.get(group);
        if(vals==null) {
            vals = new ArrayList<Object>(specs.size());
            for(int i=0; i<specs.size(); i++) {
                vals.add(null);
            }
        }
        for(int i=0; i<specs.size(); i++) {
            try {
                CombinerSpec spec = specs.get(i);
                Fields specArgFields = argFields.get(i);
                Object val;
                IFn init_fn = init_fns.get(i);
                if(specArgFields==null) {
                    val = init_fn.invoke();
                } else {
                    Tuple args = fc.getArguments().selectTuple(specArgFields);
                    ISeq toApply = Util.coerceFromTuple(args);
                    if(sortArgs!=null) {
                        toApply = RT.cons(Util.coerceFromTuple(sortArgs), toApply);
                    }
                    val = init_fn.applyTo(toApply);
                }
                if(vals.get(i)!=null) {
                    val = combiner_fns.get(i).invoke(vals.get(i), val);
                }
                vals.set(i, val);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        combined.put(group, vals);

        if(combined.size()>=this.cacheSize) {
            Tuple evict = combined.keySet().iterator().next();
            List<Object> removing = combined.remove(evict);
            write(evict, removing, fc);
        }
    }

    protected abstract void write(Tuple group, List<Object> val, OperationCall opCall);

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        TupleEntryCollector output = ((FunctionCall)operationCall).getOutputCollector();
        for(Entry<Tuple, List<Object>> e: combined.entrySet()) {
            write(e.getKey(), e.getValue(), operationCall);
        }
    }
}