package cascalog;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import java.io.File;
import java.io.IOException;

public class StdoutTap extends Lfs implements FlowListener {
    public StdoutTap() {
        super(new SequenceFile(Fields.ALL), getTempDir());
    }

    public static String getTempDir() {
        final File temp;
        try {
        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        temp.deleteOnExit();
        if(!(temp.delete())) {
            throw new RuntimeException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        return temp.getAbsoluteFile().getPath();
    }

    public void onStarting(Flow flow) {
        
    }

    public void onStopping(Flow flow) {

    }

    public void onCompleted(Flow flow) {
        try {
            TupleEntryIterator it = this.openForRead(flow.getJobConf());
            System.out.println("");
            System.out.println("");
            System.out.println("RESULTS");
            System.out.println("-----------------------");
            while(it.hasNext()) {
                System.out.println(it.next().getTuple());
            }
            System.out.println("-----------------------");
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public boolean onThrowable(Flow flow, Throwable thrwbl) {
        return false;
    }
}