package cascalog.hadoop;

import cascading.kryo.Kryo;
import cascading.kryo.KryoSerialization;
import cascading.tuple.Comparison;
import clojure.lang.Util;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import static carbonite.JavaBridge.enhanceRegistry;

/** User: sritchie Date: 12/1/11 Time: 12:21 PM */
public class ClojureKryoSerialization extends KryoSerialization implements Comparison {

    public ClojureKryoSerialization() {
        super();
    }

    public ClojureKryoSerialization(Configuration conf) {
        super(conf);
    }

    public Kryo decorateKryo(Kryo k) {
        try {
            enhanceRegistry(k);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        k.register(ArrayList.class);
        k.register(HashMap.class);
        k.register(HashSet.class);
        k.register(byte[].class);
        return k;
    }

    public Comparator getComparator( Class type ) {
        return new Comparator() {
            @Override public int compare(Object o1, Object o2) {
                return Util.compare(o1, o2);
            }
        };
    }
}
