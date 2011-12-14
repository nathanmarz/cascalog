package cascalog.hadoop;

import clojure.lang.Util;

import java.io.Serializable;
import java.util.Comparator;

/** User: sritchie Date: 12/12/11 Time: 3:23 PM */
public class DefaultComparator implements Comparator, Serializable {

    public int compare(Object o1, Object o2) {
        return Util.compare(o1, o2);
    }
}
