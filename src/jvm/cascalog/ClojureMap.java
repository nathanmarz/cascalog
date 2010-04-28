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

import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import clojure.lang.ISeq;

public class ClojureMap extends ClojureCascadingBase implements Function {  
  public ClojureMap(Fields out_fields, Object[] fn_spec, boolean stateful) {
    super(out_fields, fn_spec, stateful);
  }
  

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
    ISeq fn_args_seq = Util.coerceFromTuple(fn_call.getArguments().getTuple());
    Object res = applyFunction(fn_args_seq);
    fn_call.getOutputCollector().add(Util.coerceToTuple(res));
  }
}
