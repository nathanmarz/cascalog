package cascalog.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.KryoInstantiator;

import static carbonite.JavaBridge.defaultRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ClojureKryoInstantiator extends KryoInstantiator {

  @Override
  public Kryo newKryo() {
    try {
      Kryo k = defaultRegistry();

      k.register(ArrayList.class);
      k.register(HashMap.class);
      k.register(HashSet.class);

      k.setReferences(false);
      k.setClassLoader(Thread.currentThread().getContextClassLoader());

      return k;
    } catch (Exception e) {
      throw new RuntimeException("unable to create new Kryo: " + e);
    }
  }
}
