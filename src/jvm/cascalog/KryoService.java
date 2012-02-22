package cascalog;

import cascading.kryo.Kryo;
import cascading.kryo.KryoFactory;
import cascalog.hadoop.ClojureKryoSerialization;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.log4j.Logger;

/** User: sritchie Date: 12/16/11 Time: 8:34 PM */
public class KryoService {
    public static final Logger LOG = Logger.getLogger(KryoService.class);

    private static ObjectBuffer freshBuffer() {
        Kryo k = new ClojureKryoSerialization().populatedKryo();
        k.setRegistrationOptional(true);
        return  KryoFactory.newBuffer(k);
    }

    public static byte[] serialize(Object obj) {
        LOG.debug("Serializing " + obj);
        return freshBuffer().writeClassAndObject(obj);
    }

    public static Object deserialize(byte[] serialized) {
        Object o = freshBuffer().readClassAndObject(serialized);
        LOG.debug("Deserialized " + o);
        return o;
    }
}
