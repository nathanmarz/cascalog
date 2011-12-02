package cascalog.hadoop;

import carbonite.JavaBridge;
import cascading.kryo.KryoSerialization;
import com.esotericsoftware.kryo.Kryo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/** User: sritchie Date: 12/1/11 Time: 12:21 PM */
public class ClojureKryoSerialization extends KryoSerialization {

    @Override public Kryo makeKryo() {
        Kryo k = new Kryo();
        JavaBridge.enhanceRegistry(k);
        k.register(ArrayList.class);
        k.register(HashMap.class);
        k.register(HashSet.class);
        return k;
    }
}
