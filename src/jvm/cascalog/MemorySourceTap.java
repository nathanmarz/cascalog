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

import cascading.scheme.Scheme;
import cascading.tap.SourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class MemorySourceTap extends SourceTap {
    public static class MemorySourceScheme extends Scheme {

        private List<Tuple> tuples;
        private String id;

        public MemorySourceScheme(List<Tuple> tuples, Fields fields) {
            super(fields);
            this.tuples = tuples;
        }

        @Override
        public void sourceInit(Tap tap, JobConf jc) throws IOException {
            FileInputFormat.setInputPaths(jc, "/" + UUID.randomUUID().toString());
            jc.setInputFormat(TupleMemoryInputFormat.class);
            TupleMemoryInputFormat.storeTuples(jc, TupleMemoryInputFormat.TUPLES_PROPERTY, this.tuples);
        }

        @Override
        public void sinkInit(Tap tap, JobConf jc) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Tuple source(Object o, Object o1) {
            return ((TupleWrapper) o).tuple;
        }

        @Override
        public void sink(TupleEntry te, OutputCollector oc) throws IOException {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

    private String id = UUID.randomUUID().toString();

    public MemorySourceTap(List<Tuple> tuples, Fields fields) {
        super(new MemorySourceScheme(tuples, fields));
    }

    @Override
    public Path getPath() {
        return new Path("/" + id);
    }

    @Override
    public boolean pathExists(JobConf jc) throws IOException {
        return true;
    }

    @Override
    public long getPathModified(JobConf jc) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object object) {
        if (!getClass().equals(object.getClass())) {
            return false;
        }
        MemorySourceTap other = (MemorySourceTap) object;
        return id.equals(other.id);
    }


}
