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

package cascalog.test;

import cascalog.CascalogBuffer;
import cascading.operation.BufferCall;
import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;

public class OneBuffer extends CascalogBuffer {
  public void operate(FlowProcess flowProcess, BufferCall bufCall) {
      bufCall.getOutputCollector().add(new Tuple(1));
  }
}