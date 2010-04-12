package cascalog;

import cascading.scheme.Scheme;
import cascading.tap.SourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.FileInputFormat;


public class MemorySourceTap extends SourceTap {
    public static class MemorySourceScheme extends Scheme {

        private List<Tuple> tuples;
        private String id;

        public MemorySourceScheme(List<Tuple> tuples) {
            super(Fields.ALL);
            this.tuples = tuples;
        }

        @Override
        public void sourceInit(Tap tap, JobConf jc) throws IOException {
            FileInputFormat.setInputPaths( jc, "/" + UUID.randomUUID().toString());
            jc.setInputFormat(TupleMemoryInputFormat.class);
            TupleMemoryInputFormat.setObject(jc, TupleMemoryInputFormat.TUPLES_PROPERTY, this.tuples);
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

    public MemorySourceTap(List<Tuple> tuples) {
        super(new MemorySourceScheme(tuples));
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
        if(!getClass().equals(object.getClass())) {
            return false;
        }
        MemorySourceTap other = (MemorySourceTap) object;
        return id.equals(other.id);
    }

    

}
