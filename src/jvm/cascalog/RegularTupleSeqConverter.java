package cascalog;

import cascading.tuple.Tuple;
import clojure.lang.ISeq;
import java.util.Iterator;

public class RegularTupleSeqConverter implements Iterator<ISeq> {
    private Iterator<Tuple> _tuples;

    public RegularTupleSeqConverter(Iterator<Tuple> tuples) {
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