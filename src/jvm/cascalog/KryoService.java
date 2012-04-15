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

import cascading.kryo.Kryo;
import cascading.kryo.KryoFactory;
import cascalog.hadoop.ClojureKryoSerialization;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.log4j.Logger;

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
