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

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Lfs;
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
            TupleEntryIterator it = flow.openTapForRead(this);
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
