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
