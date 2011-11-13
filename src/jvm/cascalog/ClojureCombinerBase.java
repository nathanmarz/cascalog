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
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;

public abstract class ClojureCombinerBase extends BaseOperation implements Function {
    private List<CombinerSpec> specs;
    private Fields groupFields;
    private Fields sortFields;
    private List<Fields> argFields;
    private List<IFn> init_fns = null;
    private List<IFn> combiner_fns = null;
    private boolean includeSort;
    private String cacheConfArg;
    private int defaultCacheSize;
    private int cacheSize;

    private static Fields appendFields(Fields start, Fields... rest) {
        for(Fields f: rest) {
            if(f!=null) {
                start = start.append(f);
            }
         }
         return start;
    }

    public ClojureCombinerBase(Fields groupFields, boolean includeSort, Fields sortFields, List<Fields> argFields, Fields outFields, List<CombinerSpec> agg_specs, String cacheConfArg, int defaultCacheSize) {
        super(appendFields(groupFields, sortFields, outFields));
        if(argFields.size()!=agg_specs.size())
            throw new IllegalArgumentException("All lists to ClojureCombiner must be same length");
        this.specs = new ArrayList<CombinerSpec>(agg_specs);
        this.groupFields = groupFields;
        this.sortFields = sortFields;
        this.includeSort = includeSort;
        this.argFields = new ArrayList<Fields>(argFields);
        this.cacheConfArg = cacheConfArg;
        this.defaultCacheSize = defaultCacheSize;
    }

    //map from grouping to combiner to combined tuple
    LinkedHashMap<Tuple, Map<Integer, ISeq>> combined;

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        JobConf jc = ((HadoopFlowProcess) flowProcess).getJobConf();
        this.cacheSize = jc.getInt(this.cacheConfArg, this.defaultCacheSize);
        combined = new LinkedHashMap<Tuple, Map<Integer, ISeq>>(1000, (float) 0.75, true);
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
        Map<Integer, ISeq> vals = combined.get(group);
        if(vals==null) {
            vals = new HashMap<Integer, ISeq>(specs.size());
            combined.put(group, vals);
        }
        for(int i=0; i<specs.size(); i++) {
            try {
                Fields specArgFields = argFields.get(i);
                ISeq val;
                IFn init_fn = init_fns.get(i);
                if(specArgFields==null) {
                    val = Util.coerceToSeq(init_fn.invoke());
                } else {
                    Tuple args = fc.getArguments().selectTuple(specArgFields);
                    ISeq toApply = Util.coerceFromTuple(args);
                    if(sortArgs!=null) {
                        toApply = RT.cons(Util.coerceFromTuple(sortArgs), toApply);
                    }
                    val = Util.coerceToSeq(init_fn.applyTo(toApply));
                }
                if(vals.get(i)!=null) {
                    ISeq existing = vals.get(i);
                    val = Util.coerceToSeq(combiner_fns.get(i).applyTo(Util.cat(existing, val)));
                }
                vals.put(i, val);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        combined.put(group, vals);

        if(combined.size()>=this.cacheSize) {
            Tuple evict = combined.keySet().iterator().next();
            Map<Integer, ISeq> removing = combined.remove(evict);
            writeMap(evict, removing, fc);
        }
    }

    private void writeMap(Tuple group, Map<Integer, ISeq> val, OperationCall opCall) {
        List<Object> toWrite = Util.seqToList(val.get(0));
        for(int i=1; i<specs.size(); i++) {
            toWrite.addAll(Util.seqToList(val.get(i)));
        }
        write(group, toWrite, opCall);
    }

    protected abstract void write(Tuple group, List<Object> val, OperationCall opCall);

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        for(Entry<Tuple, Map<Integer, ISeq>> e: combined.entrySet()) {
            writeMap(e.getKey(), e.getValue(), operationCall);
        }
    }
}
