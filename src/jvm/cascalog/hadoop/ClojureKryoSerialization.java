package cascalog.hadoop;

import cascading.kryo.KryoSerialization;
import clojure.lang.Util;
import com.esotericsoftware.kryo.Kryo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import static carbonite.JavaBridge.enhanceRegistry;

/** User: sritchie Date: 12/1/11 Time: 12:21 PM */
public class ClojureKryoSerialization extends KryoSerialization {

    @Override public Kryo makeKryo() {
        Kryo k = new Kryo();

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

    @Override public Comparator getComparator( Class type ) {
        return new Comparator() {
            @Override public int compare(Object o1, Object o2) {
                return Util.compare(o1, o2);
            }
        };
    }
}
