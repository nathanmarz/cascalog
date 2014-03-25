package cascalog.kryo;

import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;
import com.twitter.chill.config.ConfigurationException;
import com.twitter.chill.hadoop.HadoopConfig;

import static cascalog.Util.clojureConf;

public class KryoService {
  static int GUESS_THREADS_PER_CORE = 4;
  static int MAX_CACHED_KRYO = GUESS_THREADS_PER_CORE * Runtime.getRuntime().availableProcessors();

  static final Object _mutex =  new Object();
  static KryoPool _kpool = null;

  public static KryoPool defaultPool() {
    synchronized(_mutex) {
      if (_kpool == null) {
        KryoInstantiator kryoInst = new ClojureKryoInstantiator();
        _kpool = KryoPool.withByteArrayOutputStream(MAX_CACHED_KRYO, kryoInst);
      }
      return _kpool;
    }
  }

  public static byte[] serialize(Object obj) {
    return defaultPool().toBytesWithClass(obj);
  }

  public static Object deserialize(byte[] bytes) {
    return defaultPool().fromBytes(bytes);
  }

}
