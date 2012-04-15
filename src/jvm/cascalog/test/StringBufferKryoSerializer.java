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

package cascalog.test;

import java.lang.Class;
import java.lang.Object;
import java.lang.Override;
import java.lang.StringBuffer;
import java.nio.ByteBuffer;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.StringSerializer;

public class StringBufferKryoSerializer extends Serializer {

  @Override
  public void writeObjectData(ByteBuffer byteBuffer, Object o) {
      StringSerializer.put(byteBuffer,"foo");
  }

  @Override
  public StringBuffer readObjectData(ByteBuffer byteBuffer, Class type) {
      String s = StringSerializer.get(byteBuffer);
      return new StringBuffer(s);
  }

}
