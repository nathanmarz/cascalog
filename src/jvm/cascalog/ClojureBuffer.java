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
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.RT;

public class ClojureBuffer extends ClojureCascadingBase implements Buffer {

    public ClojureBuffer(Fields out_fields, Object[] fn_spec, boolean stateful) {
        super(out_fields, fn_spec, stateful);
    }

    public void operate(FlowProcess flow_process, BufferCall buff_call) {
        ISeq result_seq = RT.seq(invokeFunction(IteratorSeq
            .create(new TupleSeqConverter(buff_call.getArgumentsIterator()))));
        TupleEntryCollector collector = buff_call.getOutputCollector();
        while (result_seq != null) {
            Object obj = result_seq.first();
            collector.add(Util.coerceToTuple(obj));
            result_seq = result_seq.next();
        }
    }
}
