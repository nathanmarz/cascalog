package cascalog.kryo;

import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;
import com.twitter.chill.hadoop.HadoopConfig;
import com.twitter.chill.config.ConfiguredInstantiator;
import com.twitter.chill.config.ConfigurationException;

import static cascalog.Util.clojureConf;

public class KryoService {
  static int GUESS_THREADS_PER_CORE = 4;
  static int MAX_CACHED_KRYO = GUESS_THREADS_PER_CORE * Runtime.getRuntime().availableProcessors();

  static final Object mutex =  new Object();
  static KryoPool kpool = null;

  public static KryoPool defaultPool() {
    synchronized(mutex) {
      if (kpool == null) {
        try {
          KryoInstantiator kryoInst = new ConfiguredInstantiator(new HadoopConfig(clojureConf()));
          kpool = KryoPool.withByteArrayOutputStream(MAX_CACHED_KRYO, kryoInst);
        } catch (ConfigurationException cx) {
          throw new RuntimeException(cx);
        }
      }
      return kpool;
    }
  }

  public static byte[] serialize(Object obj) {
    return defaultPool().toBytesWithClass(obj);
  }

  public static Object deserialize(byte[] bytes) {
    return defaultPool().fromBytes(bytes);
  }

}
