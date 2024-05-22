package edu.mit.ll.graphulo_ocean;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.util.StatusLogger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Loads a CSV file with two entries per row delimitted by ','.
 * First entry goes into the Accumulo column qualifier; second entry goes into the Accumulo value.
 * The Accumulo row is the file name
 */
public class CSVIngester {
  private static final Logger log = LoggerFactory.getLogger(CSVIngester.class);

  private Connector connector;

  public CSVIngester(Connector connector) {
    this.connector = connector;
  }

  public long ingestFile(File file, String Atable, boolean deleteIfExists) throws IOException {
    return ingestFile(file, Atable, deleteIfExists, 1, 0);
  }

  public long ingestFile(File file, String Atable, boolean deleteIfExists,
                         int everyXLines, int startOffset) throws IOException {
    Preconditions.checkArgument(everyXLines >= 1 && startOffset >= 0, "bad params ", everyXLines, startOffset);
    if (deleteIfExists && connector.tableOperations().exists(Atable))
      try {
        connector.tableOperations().delete(Atable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.warn("trouble deleting table "+Atable, e);
        throw new RuntimeException(e);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    if (!connector.tableOperations().exists(Atable))
      try {
        connector.tableOperations().create(Atable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.warn("trouble creating table " + Atable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        throw new RuntimeException(e);
      }

    String sampleid0 = file.getName();
    if (sampleid0.endsWith(".csv"))
      sampleid0 = sampleid0.substring(0, sampleid0.length()-4);
    Text sampleid = new Text(sampleid0);

    BatchWriter bw = null;
    String line = null;
    long entriesProcessed = 0;

    try (BufferedReader fo = new BufferedReader(new FileReader(file))) {
      BatchWriterConfig config = new BatchWriterConfig();
      bw = connector.createBatchWriter(Atable, config);

      // Skip header line
      fo.readLine();
      // Offset
      for (int i = 0; i < startOffset; i++)
        fo.readLine();

      // log every 2 minutes
      StatusLogger slog = new StatusLogger();
      String partialMsg = file.getName()+": entries processed: ";

      long linecnt = 0;
      while ((line = fo.readLine()) != null)
        if (!line.isEmpty() && linecnt++ % everyXLines == 0) {
          entriesProcessed += ingestLine(sampleid, bw, line);
          slog.logPeriodic(log, partialMsg+entriesProcessed);
        }

    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    } catch (MutationsRejectedException e) {
      log.warn("Mutation rejected on line "+line, e);
    } finally {
      if (bw != null)
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          log.warn("Mutation rejected at close() on line "+line, e);
        }
    }
    return entriesProcessed;
  }

//  static final Text EMPTY_TEXT = new Text();

  protected long ingestLine(Text row, BatchWriter bw, String line) throws MutationsRejectedException {
    String[] parts = line.split(",");
    if (parts.length != 2) {
      log.error("Bad CSV line: "+line);
      return 0;
    }

    String seqid = parts[0];
    String seq = parts[1];

//    Text seqidtext = new Text(seqid);

    Mutation m = new Mutation(row);
    m.put(EMPTY_BYTES, seqid.getBytes(UTF_8), seq.getBytes(UTF_8));
    bw.addMutation(m);

    return 1;
  }

}
