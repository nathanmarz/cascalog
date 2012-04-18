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
