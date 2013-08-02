/*
    Copyright 2010 Nathan Marz
 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package cascalog;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.log4j.Logger;

import cascalog.hadoop.ClojureKryoSerialization;

public class KryoService {

  private static final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>();
  private static final ThreadLocal<ByteArrayOutputStream> byteStream =
      new ThreadLocal<ByteArrayOutputStream>();

  public static final Logger LOG = Logger.getLogger(KryoService.class);

  public static Kryo getKryo() {
    if (kryo.get() == null) { kryo.set(freshKryo()); }

    return kryo.get();
  }

  public static ByteArrayOutputStream getByteStream() {
    if (byteStream.get() == null) { byteStream.set(new ByteArrayOutputStream()); }

    return byteStream.get();
  }

  private static Kryo freshKryo() {
    Kryo k = new ClojureKryoSerialization().populatedKryo();
    k.setRegistrationRequired(false);
    return k;
  }

  public static byte[] serialize(Object obj) {
    LOG.debug("Serializing " + obj);
    getByteStream().reset();
    Output ko = new Output(getByteStream());
    getKryo().writeClassAndObject(ko, obj);
    ko.flush();
    byte[] bytes = getByteStream().toByteArray();

    return bytes;
  }

  public static Object deserialize(byte[] serialized) {
    Object o = getKryo().readClassAndObject(new Input(serialized));
    LOG.debug("Deserialized " + o);
    return o;
  }
}
