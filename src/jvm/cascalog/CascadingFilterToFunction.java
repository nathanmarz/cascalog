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
import cascading.operation.Filter;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.operation.BaseOperation;



public class CascadingFilterToFunction extends BaseOperation implements Function {  
  Filter filter;

  public CascadingFilterToFunction(String outfield, Filter filter) {
      super(new Fields(outfield));
      this.filter = filter;
  }
  
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    filter.prepare(flowProcess, operationCall);
  }

  public void operate(FlowProcess process, FunctionCall call) {
    boolean ret = !filter.isRemove(process, new FilterFunctionCall(call));
    call.getOutputCollector().add(new Tuple(ret));
  }
  
  public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
    filter.cleanup(flowProcess, operationCall);
  }

}