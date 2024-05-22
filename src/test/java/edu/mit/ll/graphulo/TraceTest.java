package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Test distributed tracing
 */
public class TraceTest extends AccumuloTestBase {
  private static final Logger log = LoggerFactory.getLogger(TraceTest.class);

  @Test
  public void printTraceConfig() {
    ClientConfiguration cc = ClientConfiguration.loadDefault();
    System.out.println("trace.span.receiver ClientConfiguratio default:");
    System.out.println("TRACE_ZK_PATH  "+cc.get(ClientConfiguration.ClientProperty.TRACE_ZK_PATH));
    System.out.println("TRACE_SPAN_RECEIVERS  " + cc.get(ClientConfiguration.ClientProperty.TRACE_SPAN_RECEIVERS));
    TestUtil.printIterator(cc.getAllPropertiesWithPrefix(ClientConfiguration.ClientProperty.TRACE_SPAN_RECEIVER_PREFIX).entrySet().iterator());
  }

  @Test
  public void test1() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException, InterruptedException {
    Connector conn = tester.getConnector();
    final String tA;
    {
      String[] names = getUniqueNames(1);
      tA = "testTrace1"; //names[0];
    }

    TableOperations tops = conn.tableOperations();

    if (!tops.exists(tA))
      tops.create(tA);
    Scanner scanner = conn.createScanner(tA, Authorizations.EMPTY);
    BatchWriter bw = conn.createBatchWriter(tA, new BatchWriterConfig());


//    ClientConfiguration cc = ClientConfiguration.loadDefault();
//    String spanrcv = cc.get(ClientConfiguration.ClientProperty.TRACE_SPAN_RECEIVERS);


    DistributedTrace.enable(null, "TraceTest"); // service name. hostname is auto-determined

    try {
      try (TraceScope span = Trace.startSpan("readStep", Sampler.ALWAYS)) {
        System.out.println("read before: ");
        for (Map.Entry<Key, Value> entry : scanner) {
          System.out.println(entry.getKey().toStringNoTime() + "    " + entry.getValue());
        }
      }

      try (TraceScope span = Trace.startSpan("writeStep", Sampler.ALWAYS)) {
        Mutation m = new Mutation("arow");
        m.put("acf", "acq", "aval");
        bw.addMutation(m);
        bw.flush();
      }

      try (TraceScope span = Trace.startSpan("readStep", Sampler.ALWAYS)) {
        System.out.println("read after: ");
        for (Map.Entry<Key, Value> entry : scanner) {
          System.out.println(entry.getKey().toStringNoTime() + "    " + entry.getValue());
          Assert.assertEquals("arow", entry.getKey().getRow().toString());
          Assert.assertEquals("acf", entry.getKey().getColumnFamily().toString());
          Assert.assertEquals("acq", entry.getKey().getColumnQualifier().toString());
          Assert.assertEquals("", entry.getKey().getColumnVisibility().toString());
          Assert.assertEquals("aval", entry.getValue().toString());
        }
        Thread.sleep(100);
      }

    } finally {
      DistributedTrace.disable();
      bw.close();
      scanner.close();
    }
  }
}
