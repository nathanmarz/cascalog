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
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;

public class ClojureFilter extends BaseOperation implements Filter {
  private Object[] fn_spec;
  private IFn fn;
  
  public ClojureFilter(Object[] fn_spec) {
    this.fn_spec = fn_spec;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.fn = Util.bootFn(fn_spec);
  }

  public boolean isRemove(FlowProcess flow_process, FilterCall filter_call) {
    ISeq fn_args_seq = Util.coerceFromTuple(filter_call.getArguments().getTuple());
    try {
      return !Util.truthy(this.fn.applyTo(fn_args_seq));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
