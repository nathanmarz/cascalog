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