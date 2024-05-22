package edu.mit.ll.graphulo.tricount;

import edu.mit.ll.graphulo.reducer.ReducerSerializable;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public final class OneAggReducer extends ReducerSerializable<Long> {


  private long triangles = 0L;


  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) {
  }

  @Override
  public void reset() throws IOException {
    triangles = 0L;
  }

  @Override
  public void update(Key k, Value v) {
    triangles += GraphuloUtil.readUnsignedVLong(v.get()); // empty counts as 0
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
