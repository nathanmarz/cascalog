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
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.operation.BaseOperation;


public class CascalogFunctionExecutor extends BaseOperation implements Function {  
  CascalogFunction fn;

  public CascalogFunctionExecutor(Fields out_fields, CascalogFunction fn) {
    super(out_fields);
    this.fn = fn;
  }
  
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    fn.prepare(flowProcess, operationCall);
  }

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
    fn.operate(flow_process, fn_call);
  }
  
  public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
    fn.cleanup(flowProcess, operationCall);
  }

}