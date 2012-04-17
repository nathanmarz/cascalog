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

public class ClojureCascadingBase extends BaseOperation {
    private byte[] serialized_fn;
    private IFn fn;

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
    public void prepare(FlowProcess flow_process, OperationCall op_call) {
        this.fn = Util.deserializeFn(serialized_fn);
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
    public void cleanup(FlowProcess flowProcess, OperationCall op_call) {
        //TODO: how to manage cleanup...
    }
}
