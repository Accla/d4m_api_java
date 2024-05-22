package edu.mit.ll.graphulo_ndsi;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static edu.mit.ll.graphulo_ndsi.NDSIGraphulo.PADSIZE_LATLON;

/**
 * Ingest the NDSI dataset from a file into Accumulo.
 */
public class NDSIIngester {
  private static final Logger log = LoggerFactory.getLogger(NDSIIngester.class);

  private Connector connector;

  public NDSIIngester(Connector connector) {
    this.connector = connector;
  }

  /**
   * Insert the contents of an NDSI data file into an Accumulo table.
   *
   * @param file Input file
   * @param Atable Table to insert into
   * @return Nummber of entries inserted into Atable; equal to 2x the number of rows.
   * @throws IOException
   */
  public long ingestFile(File file, String Atable, boolean deleteIfExists) throws IOException {
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

    BatchWriter bw = null;
    String line = null;
    long entriesProcessed = 0;

    try (BufferedReader fo = new BufferedReader(new FileReader(file))) {
      BatchWriterConfig config = new BatchWriterConfig();
      bw = connector.createBatchWriter(Atable, config);

      // Skip header line
      fo.readLine();

      while ((line = fo.readLine()) != null)
        if (!line.isEmpty())
          entriesProcessed += ingestLine(bw, line);

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

  static final Text COLF_NDSI = new Text("ndsi");
  static final Text COLF_LSM = new Text("lsm");

  private int ingestLine(BatchWriter bw, String line) throws MutationsRejectedException {
    String[] parts = line.split(",");
    String longitude = StringUtils.leftPad(parts[0], PADSIZE_LATLON, '0');
    String latitude = StringUtils.leftPad(parts[1], PADSIZE_LATLON, '0');
    String ndsi = parts[2];
    //String ndsi_count = parts[3];
    String land_sea_mask = parts[4];

    Text longText = new Text(longitude);
    Text latText = new Text(latitude);

    Mutation m = new Mutation(longText);
    m.put(COLF_NDSI, latText, new Value(ndsi.getBytes(StandardCharsets.UTF_8)));
    m.put(COLF_LSM, latText, new Value(land_sea_mask.getBytes(StandardCharsets.UTF_8)));
    bw.addMutation(m);

    return 2;
  }

}
