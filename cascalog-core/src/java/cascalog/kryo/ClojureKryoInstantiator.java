package cascalog.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.config.Config;
import com.twitter.chill.config.ReflectingInstantiator;
import com.twitter.chill.config.ConfigurationException;
import org.objenesis.strategy.StdInstantiatorStrategy;

import static carbonite.JavaBridge.enhanceRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ClojureKryoInstantiator extends KryoInstantiator {

  final Config config;
  final ReflectingInstantiator reflectingInst;

  public ClojureKryoInstantiator(Config config) {
    try {
      this.config = config;
      reflectingInst = new ReflectingInstantiator(config);
    } catch (ConfigurationException cx) {
      throw new RuntimeException(cx);
    }
  }

  @Override
  public Kryo newKryo() {
    try {
      Kryo k = reflectingInst.newKryo();
      // register all the carbonite serializers
      enhanceRegistry(k);

      k.register(ArrayList.class);
      k.register(HashMap.class);
      k.register(HashSet.class);

      k.setReferences(config.getBoolean("cascalog.kryo.setreferences", false));
      k.setClassLoader(Thread.currentThread().getContextClassLoader());

      return k;

    } catch (Exception e) {
      throw new RuntimeException("unable to create new Kryo: " + e);
    }
  }
}
