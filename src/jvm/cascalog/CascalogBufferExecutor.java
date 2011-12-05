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
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;


public class CascalogBufferExecutor extends BaseOperation implements Buffer {  
  CascalogBuffer buf;

  public CascalogBufferExecutor(Fields outFields, CascalogBuffer buf) {
    super(outFields);
    this.buf = buf;
  }
  
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    buf.prepare(flowProcess, operationCall);
  }

  public void operate(FlowProcess flowProcess, BufferCall bufCall) {
    buf.operate(flowProcess, bufCall);
  }
  
  public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
    buf.cleanup(flowProcess, operationCall);
  }

}