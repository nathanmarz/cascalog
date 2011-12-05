package cascalog;

import cascading.tuple.TupleEntry;
import clojure.lang.ISeq;

import java.util.Iterator;

public class TupleSeqConverter implements Iterator<ISeq> {
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