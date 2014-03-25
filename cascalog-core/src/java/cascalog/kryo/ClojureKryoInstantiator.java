package cascalog.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.config.Config;
import org.objenesis.strategy.StdInstantiatorStrategy;

import static carbonite.JavaBridge.defaultRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ClojureKryoInstantiator extends KryoInstantiator {

  Config config;

  public ClojureKryoInstantiator(Config config) {
    this.config = config;
  }

  @Override
  public Kryo newKryo() {
    try {
      Kryo k = defaultRegistry();

      k.register(ArrayList.class);
      k.register(HashMap.class);
      k.register(HashSet.class);

      k.setRegistrationRequired(config.getBoolean("cascalog.kryo.registrationrequired", false));
      k.setInstantiatorStrategy(new StdInstantiatorStrategy());
      k.setReferences(config.getBoolean("cascalog.kryo.setreferences", false));
      k.setClassLoader(Thread.currentThread().getContextClassLoader());

      return k;

    } catch (Exception e) {
      throw new RuntimeException("unable to create new Kryo: " + e);
    }
  }
}
