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
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import java.util.Map;

public class ClojureCascadingBase extends BaseOperation {
    private byte[] serialized_fn;
    private IFn fn;
    private IFn cleanupFn;

    private void init(IFn fn) {
        serialized_fn = Util.serializeFn(fn); 
        // test to make sure it's deserializable
        Util.deserializeFn(serialized_fn);
    }
    
    public ClojureCascadingBase(IFn fn) {
        init(fn);
    }
    
    public ClojureCascadingBase(Fields fields, IFn fn) {
        super(fields);
        init(fn);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall opCall) {
        IFn fn = Util.deserializeFn(serialized_fn);
        Boolean isPrepared = (Boolean) Util.bootSimpleFn("cascalog.workflow", "prepared?").invoke(fn);  
        
        if(isPrepared.booleanValue()) {
            Object res = fn.invoke(flowProcess, opCall);
            if(res instanceof Map) {
                Map resmap = (Map) res;
                this.fn = (IFn) resmap.get(Keyword.intern("operate"));
                this.cleanupFn = (IFn) resmap.get(Keyword.intern("cleanup"));
            } else {
                this.fn = (IFn) res;
                this.cleanupFn = null;
            }
        } else {
            this.fn = fn;
            this.cleanupFn = null;
        }
    }

    protected Object applyFunction(ISeq seq) {
        try {
            return this.fn.applyTo(seq);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Object invokeFunction(Object arg) {
        try {
            return this.fn.invoke(arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    protected Object invokeFunction() {
        try {
            return this.fn.invoke();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall opCall) {
        if(cleanupFn!=null) {
            cleanupFn.invoke();
        }
    }
}
