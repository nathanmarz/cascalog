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
    private byte[] serialized_spec;
    private Object[] fn_spec;

    private boolean stateful;
    private Object state;
    private IFn fn;

    public void initialize(Object[] fn_spec, boolean stateful) {
        serialized_spec = KryoService.serialize(fn_spec);
        this.stateful = stateful;
    }

    public ClojureCascadingBase(Object[] fn_spec, boolean stateful) {
        initialize(fn_spec, stateful);
    }

    public ClojureCascadingBase(Fields fields, Object[] fn_spec, boolean stateful) {
        super(fields);
        initialize(fn_spec, stateful);
    }

    @Override
    public void prepare(FlowProcess flow_process, OperationCall op_call) {
        this.fn_spec = KryoService.deserialize(flow_process, serialized_spec);
        this.fn = Util.bootFn(fn_spec);
        if (stateful) {
            try {
                state = this.fn.invoke();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected Object applyFunction(ISeq seq) {
        try {
            if (stateful) {
                return this.fn.applyTo(seq.cons(state));
            } else {
                return this.fn.applyTo(seq);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Object invokeFunction(Object arg) {
        try {
            if (stateful) {
                return this.fn.invoke(state, arg);
            } else {
                return this.fn.invoke(arg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    protected Object invokeFunction() {
        try {
            if (stateful) {
                return this.fn.invoke(state);
            } else {
                return this.fn.invoke();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall op_call) {
        if (stateful) {
            try {
                this.fn.invoke(state);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
