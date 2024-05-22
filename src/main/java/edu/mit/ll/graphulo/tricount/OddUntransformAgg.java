package edu.mit.ll.graphulo.tricount;

import edu.mit.ll.graphulo.reducer.ReducerSerializable;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;

/**
 * Column "in|v3" ==> "v3".
 * Stores a set of the columns reached in one step of BFS on the incidence matrix.
 * Pass as an option a D4M string of all acceptable prefixes, e.g., "inA|,inB|,".
 */
public class OddUntransformAgg extends ReducerSerializable<Long> {
  private static final Logger log = LoggerFactory.getLogger(OddUntransformAgg.class);

  private long triangles = 0L;
  private static final Lexicoder<Integer> UINTEGER_LEXICODER = new UIntegerLexicoder();

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) {
  }

  @Override
  public void reset() throws IOException {
    triangles = 0L;
  }

  @Override
  public void update(Key k, Value v) {
    int tri = UINTEGER_LEXICODER.decode(v.get());
    if( tri % 2 == 0 )
      return;
    triangles += (tri-1)/2;
  }

  @Override
  public void combine(Long another) {
    triangles += another;
  }

  @Override
  public boolean hasTopForClient() {
    return triangles != 0;
  }

  @Override
  public Long getSerializableForClient() {
    return triangles;
  }
}
