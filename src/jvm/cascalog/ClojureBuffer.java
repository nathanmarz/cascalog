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
import cascading.operation.Buffer;
import cascading.operation.OperationCall;
import cascading.operation.BufferCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import java.util.Iterator;
import clojure.lang.RT;

 
public class ClojureBuffer extends BaseOperation<Object>
                           implements Buffer<Object> {
  private Object[] fn_spec;
  private IFn fn;

  protected static class TupleSeqConverter implements Iterator<ISeq> {
        private Iterator<TupleEntry> _tuples;

        public TupleSeqConverter(Iterator<TupleEntry> tuples) {
            _tuples = tuples;
        }

        public boolean hasNext() {
            return _tuples.hasNext();
        }

        public ISeq next() {
            return Util.coerceFromTuple(_tuples.next());
        }

        public void remove() {
            _tuples.remove();
        }      
  }
 
  public ClojureBuffer(Fields out_fields, Object[] fn_spec) {
    super(out_fields);
    this.fn_spec = fn_spec;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall<Object> op_call) {
    this.fn = Util.bootFn(fn_spec);
  }
 
  public void operate(FlowProcess flow_process, BufferCall<Object> buff_call) {
    try {
      ISeq result_seq = RT.seq(this.fn.invoke(IteratorSeq.create(new TupleSeqConverter(buff_call.getArgumentsIterator()))));
      TupleEntryCollector collector = buff_call.getOutputCollector();
      while (result_seq != null) {
         Object obj = result_seq.first();
         collector.add(Util.coerceToTuple(obj));
         result_seq = result_seq.next();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
