package cascalog;

import cascading.CascadingException;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.FlowProcess;
import cascading.kryo.Kryo;
import cascading.kryo.KryoFactory;
import cascalog.hadoop.ClojureKryoSerialization;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.lang.NullPointerException;
import java.lang.Object;
import java.lang.String;
import java.lang.System;
import java.lang.reflect.Array;
import java.util.Arrays;

import clojure.lang.RT;
import clojure.lang.Var;

/** User: sritchie Date: 12/16/11 Time: 8:34 PM */
public class KryoService {
    public static final Logger LOG = Logger.getLogger(KryoService.class);
    static Var require = RT.var("clojure.core", "require");
    static Var symbol = RT.var("clojure.core", "symbol");
    static Var projectConf;
    static Var hadoopJobConf;

    static {
        ClojureKryoSerialization serialization = new ClojureKryoSerialization();
        Kryo k = serialization.populatedKryo();
        k.setRegistrationOptional(true);

        try {
            require.invoke(symbol.invoke("cascalog.conf"));
            require.invoke(symbol.invoke("hadoop-util.core"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        projectConf = RT.var("cascalog.conf", "project-conf");
        hadoopJobConf = RT.var("hadoop-util.core", "job-conf");
    }

    public static byte[] serialize(Object[] objs) {
        LOG.debug("Serializing " + objs);
        Configuration conf = (Configuration) hadoopJobConf.invoke(projectConf.invoke());
        SerializationFactory factory = new SerializationFactory(conf);

        Serializer<Object> serializer;
        try {
            serializer = factory.getSerializer(Object.class);
        } catch (NullPointerException e) {
          // for compatability with the expected behavior documented by the
          // java.util.GregorianCalendar test in cascalog.conf-test
          // TODO   or... should we change the test?
          throw new CascadingException("No serializer found", e);
        }

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            serializer.open(stream);
            serializer.serialize(objs);
            serializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return stream.toByteArray();
    }

    public static Object[] deserialize(FlowProcess flow_process, byte[] serialized) {
        Object[] objs = null;

        Configuration flowConf = null;
        if (flow_process != null && flow_process instanceof HadoopFlowProcess) {
            // flow_process is null when called from KryoInsert
            // flow_process is not an instance of HadoopFlowProcess when called via bridge-test
            flowConf = ((HadoopFlowProcess) flow_process).getJobConf();
        }
        Configuration conf = (Configuration) hadoopJobConf.invoke(projectConf.invoke(flowConf));

        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer<Object[]> deserializer = factory.getDeserializer(Object[].class);

        ByteArrayInputStream stream = new ByteArrayInputStream(serialized);
        try {
            deserializer.open(stream);
            objs = deserializer.deserialize(null);
            deserializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        LOG.debug("Deserialized " + objs);
        return objs;
    }
}
