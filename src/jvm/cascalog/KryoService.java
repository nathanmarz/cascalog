package cascalog;

import cascading.kryo.Kryo;
import cascading.kryo.KryoFactory;
import cascalog.hadoop.ClojureKryoSerialization;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.log4j.Logger;

/** User: sritchie Date: 12/16/11 Time: 8:34 PM */
public class KryoService {
    public static final Logger LOG = Logger.getLogger(KryoService.class);
    static ObjectBuffer kryoBuf;

    static {
        ClojureKryoSerialization serialization = new ClojureKryoSerialization();
        Kryo k = serialization.populatedKryo();
        k.setRegistrationOptional(true);

        kryoBuf = KryoFactory.newBuffer(k);
    }

    public static byte[] serialize(Object obj) {
        LOG.debug("Serializing " + obj);
        return kryoBuf.writeClassAndObject(obj);
    }

    public static Object deserialize(byte[] serialized) {
        Object o = kryoBuf.readClassAndObject(serialized);
        LOG.debug("Deserialized " + o);
        return o;
    }
}
