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
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import java.util.Collection;

public class ClojureMap extends BaseOperation implements Function {
  private Object[] fn_spec;
  private IFn fn;
  
  public ClojureMap(Fields out_fields, Object[] fn_spec) {
    super(out_fields);
    this.fn_spec = fn_spec;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.fn = Util.bootFn(fn_spec);
  }

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
    ISeq fn_args_seq = Util.coerceFromTuple(fn_call.getArguments().getTuple());
    try {
      Object res = this.fn.applyTo(fn_args_seq);
      fn_call.getOutputCollector().add(Util.coerceToTuple(res));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
