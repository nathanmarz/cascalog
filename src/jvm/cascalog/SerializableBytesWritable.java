package cascalog;

import org.apache.hadoop.io.BytesWritable;
import java.io.Serializable;

public class SerializableBytesWritable extends BytesWritable implements Serializable {
    public SerializableBytesWritable() {
        
    }
    
    public SerializableBytesWritable(byte[] arr) {
        super(arr);
    }
}