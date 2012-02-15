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

import cascading.tuple.Tuple;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TupleMemoryInputFormat implements InputFormat<TupleWrapper, NullWritable> {

    public static final String ENCODING = "US-ASCII";
    public static final String TUPLES_PROPERTY = "memory.format.tuples";

    public static class TupleInputSplit implements InputSplit {
        public int numTuples;

        public TupleInputSplit() {
        }

        public TupleInputSplit(int numTuples) {
            this.numTuples = numTuples;
        }

        public long getLength() throws IOException {
            return numTuples;
        }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        public void write(DataOutput d) throws IOException {
            d.writeInt(numTuples);
        }

        public void readFields(DataInput di) throws IOException {
            numTuples = di.readInt();
        }
    }

    public static class TupleRecordReader implements RecordReader<TupleWrapper, NullWritable> {

        List<Tuple> tuples;
        int pos = 0;

        public TupleRecordReader(List<Tuple> tuples) {
            this.tuples = tuples;
        }

        public boolean next(TupleWrapper k, NullWritable v) throws IOException {
            if (pos >= tuples.size()) { return false; }
            k.tuple = tuples.get(pos);
            pos++;
            return true;
        }

        public TupleWrapper createKey() {
            return new TupleWrapper();
        }

        public NullWritable createValue() {
            return NullWritable.get();
        }

        public long getPos() throws IOException {
            return pos;
        }

        public void close() throws IOException {
        }

        public float getProgress() throws IOException {
            if (tuples.size() == 0) { return 1; }
            return (float) (pos * 1.0 / tuples.size());
        }

    }

    public InputSplit[] getSplits(JobConf jc, int i) throws IOException {
        List<Tuple> tuples = retrieveTuples(jc, TUPLES_PROPERTY);
        return new InputSplit[]{new TupleInputSplit(tuples.size())};
    }

    public RecordReader<TupleWrapper, NullWritable>
    getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
        return new TupleRecordReader(retrieveTuples(jc, TUPLES_PROPERTY));
    }

    public static String encodeBytes(byte[] bytes) {
        String byteString = null;
        try {
            byteString = new String(Base64.encodeBase64(bytes), "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return byteString;
    }

    public static byte[] decodeBytes(String str) {
        byte[] decoded = null;
        try {
            byte[] bytes = str.getBytes(ENCODING);
            decoded = Base64.decodeBase64(bytes);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return decoded;
    }

    public static void storeTuples(JobConf conf, String key, List<Tuple> tuples) {
        SerializationFactory factory = new SerializationFactory(conf);
        Serializer<Tuple> serializer = factory.getSerializer(Tuple.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            serializer.open(stream);
            for(Tuple tuple: tuples) {
                serializer.serialize(tuple);
            }
            serializer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String confVal = tuples.size() + ":" + encodeBytes(stream.toByteArray());
        conf.set(key, confVal);
    }

    public static List<Tuple> retrieveTuples(JobConf conf, String key) {
        String s = conf.get(key);
        if (s == null)
            return null;

        String[] pieces = s.split(":");
        int size = Integer.valueOf(pieces[0]);
        byte[] val = decodeBytes(pieces[1]);

        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer<Tuple> deserializer = factory.getDeserializer(Tuple.class);
        ByteArrayInputStream stream = new ByteArrayInputStream(val);

        List<Tuple> ret = new ArrayList<Tuple>();
        try {
            deserializer.open(stream);
            for(int i = 0; i < size; i++) {
                ret.add(deserializer.deserialize(null));
            }
            deserializer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ret;
    }
}
