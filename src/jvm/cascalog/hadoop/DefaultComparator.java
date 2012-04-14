package cascalog.hadoop;

import cascading.tuple.hadoop.TupleSerialization;
import clojure.lang.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.io.Serializable;
import java.util.Comparator;

public class DefaultComparator extends Configured implements Comparator, Serializable {

    TupleSerialization serialization;

    public DefaultComparator() {
    }

    public DefaultComparator( Configuration conf ) {
        super( conf );
    }

    @Override public void setConf( Configuration conf ) {
        if( conf == null ) return;
        super.setConf( conf );
        serialization = new TupleSerialization( conf );
    }

    private Comparator getComparator(Class klass) {
        if (serialization == null)
            return null;

        Comparator comp = serialization.getComparator(klass);
        return (comp instanceof DefaultComparator) ? null : comp;
    }

    public int compare(Object o1, Object o2) {
        Comparator comp = getComparator(o1.getClass());
        return (comp == null) ? Util.compare(o1, o2) : comp.compare(o1, o2);
    }
}
