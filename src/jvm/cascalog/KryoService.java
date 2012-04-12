package cascalog;

import cascalog.hadoop.ClojureKryoSerialization;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;

/** User: sritchie Date: 12/16/11 Time: 8:34 PM */
public class KryoService {

    /**
     * Initial capacity of the Kryo object buffer.
     */
    private static final int INIT_CAPACITY = 2000;

    /**
     * Maximum capacity of the Kryo object buffer.
     */
    private static final int FINAL_CAPACITY = 2000000000;

    public static final Logger LOG = Logger.getLogger(KryoService.class);

    private static Kryo freshKryo() {
        Kryo k = new ClojureKryoSerialization().populatedKryo();
        k.setRegistrationRequired(false);
        return k;
    }

    public static byte[] serialize(Object obj) {
        LOG.debug("Serializing " + obj);
        Output output = new Output(INIT_CAPACITY, FINAL_CAPACITY);
        freshKryo().writeClassAndObject(output, obj);
        return output.toBytes();
    }

    public static Object deserialize(byte[] serialized) {
        Object o = freshKryo().readClassAndObject(new Input(serialized));
        LOG.debug("Deserialized " + o);
        return o;
    }
}
