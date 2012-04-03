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
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;

import java.io.Serializable;
import jcascalog.CascalogOp;

public abstract class CascalogFunction implements Serializable, CascalogOp {
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {

    }

    public abstract void operate(FlowProcess flow_process, FunctionCall fn_call);

    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {

    }
}