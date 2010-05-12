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

import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import java.util.List;

public class ClojureCombiner extends ClojureCombinerBase {
    
    public ClojureCombiner(Fields groupFields, List<Fields> argFields, List<String> outFields, List<CombinerSpec> agg_specs) {
        super(groupFields, false, null, argFields, new Fields(outFields.toArray(new String[outFields.size()])), agg_specs, 10000);
    }

    @Override
    protected void write(Tuple group, List<Object> val, OperationCall opCall) {
        TupleEntryCollector output = ((FunctionCall)opCall).getOutputCollector();
        Tuple t = new Tuple(group);
        for(Object o: val) {
            t.add(o);
        }
        output.add(t);
    }
}