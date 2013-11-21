package cascalog.moreTaps;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.lang.Override;

public class WholeFile extends
    Scheme<JobConf, RecordReader<Text, BytesWritable>, OutputCollector, Object[], Object[]> {

    public WholeFile( Fields fields ) {
        super(fields);
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess,
        Tap<JobConf, RecordReader<Text, BytesWritable>, OutputCollector> tap,
        JobConf conf) {
        conf.setInputFormat( WholeFileInputFormat.class );
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess,
        Tap<JobConf, RecordReader<Text, BytesWritable>, OutputCollector> tap,
        JobConf conf) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader<Text, BytesWritable>> sourceCall) {
        sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader<Text, BytesWritable>> sourceCall) throws IOException {


        Text key = (Text) sourceCall.getContext()[0];
        BytesWritable value = (BytesWritable) sourceCall.getContext()[1];

        boolean result = sourceCall.getInput().next(key, value);

        if (!result)
            return false;

        sourceCall.getIncomingEntry().setTuple(new Tuple(key.toString(), value));
        return true;
    }

    @Override
    public void sink(FlowProcess<JobConf>  flowProcess,
        SinkCall<Object[], OutputCollector> outputCollectorSinkCall) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
