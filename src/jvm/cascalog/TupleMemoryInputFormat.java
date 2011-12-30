/*
    Copyright 2010 Nathan Marz
 
    Project and contact information: http://www.cascalog.org/ 

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package cascalog;

import cascading.tuple.Tuple;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class TupleMemoryInputFormat implements InputFormat<TupleWrapper, NullWritable> {

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
        List<Tuple> tuples = (List<Tuple>) getObject(jc, TUPLES_PROPERTY);
        return new InputSplit[]{new TupleInputSplit(tuples.size())};
    }

    public RecordReader<TupleWrapper, NullWritable>
    getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
        return new TupleRecordReader((List<Tuple>) getObject(jc, TUPLES_PROPERTY));
    }


    public static void setObject(JobConf conf, String key, Object o) {
        conf.set(key, StringUtils.byteToHexString(KryoService.serialize(o)));
    }

    public static Object getObject(JobConf conf, String key) {
        String s = conf.get(key);
        if (s == null) { return null; }
        byte[] val = StringUtils.hexStringToByte(s);
        return KryoService.deserialize(val);
    }
}
