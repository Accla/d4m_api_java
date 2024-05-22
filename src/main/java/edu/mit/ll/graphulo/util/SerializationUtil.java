package edu.mit.ll.graphulo.util;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
//import java.util.Base64
/**
 * Partially based from {@link org.apache.commons.lang3.SerializationUtils}.
 *
 * <p>Assists with the serialization process and performs additional functionality based
 * on serialization.</p>
 */
public final class SerializationUtil {

  private SerializationUtil() {
    super();
  }

  public static String serializeWritableBase64(Writable writable) {
    byte[] b = serializeWritable(writable);
    //return org.apache.accumulo.core.util.Base64.encodeBase64String(b);
    return java.util.Base64.getEncoder().encodeToString(b);
  }

  public static void deserializeWritableBase64(Writable writable, String str) {
    byte[] b = org.apache.commons.codec.binary.Base64.decodeBase64(str);
    deserializeWritable(writable, b);
  }

  public static String serializeBase64(Serializable obj) {
    byte[] b = serialize(obj);
    //return org.apache.accumulo.core.util.Base64.encodeBase64String(b);
    return java.util.Base64.getEncoder().encodeToString(b);
  }

  public static Object deserializeBase64(String str) {
    byte[] b = Base64.decodeBase64(str);
    return deserialize(b);
  }


  // Interop with Hadoop Writable
  //-----------------------------------------------------------------------

  public static byte[] serializeWritable(Writable writable) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
    serializeWritable(writable, baos);
    return baos.toByteArray();
  }

  public static void serializeWritable(Writable obj, OutputStream outputStream) {
    Preconditions.checkNotNull(obj);
    Preconditions.checkNotNull(outputStream);
    try (DataOutputStream out = new DataOutputStream(outputStream)) {
      obj.write(out);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void deserializeWritable(Writable writable, InputStream inputStream) {
    Preconditions.checkNotNull(writable);
    Preconditions.checkNotNull(inputStream);
    try (DataInputStream in = new DataInputStream(inputStream)) {
      writable.readFields(in);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void deserializeWritable(Writable writable, byte[] objectData) {
    Preconditions.checkNotNull(objectData);
    deserializeWritable(writable, new ByteArrayInputStream(objectData));
  }



  // Serialize
  //-----------------------------------------------------------------------

  /**
   * <p>Serializes an {@code Object} to the specified stream.</p>
   * <p/>
   * <p>The stream will be closed once the object is written.
   * This avoids the need for a finally clause, and maybe also exception
   * handling, in the application code.</p>
   * <p/>
   * <p>The stream passed in is not buffered internally within this method.
   * This is the responsibility of your application if desired.</p>
   *
   * @param obj          the object to serialize to bytes, may be null
   * @param outputStream the stream to write to, must not be null
   * @throws IllegalArgumentException if {@code outputStream} is {@code null}
   */
  public static void serialize(Serializable obj, OutputStream outputStream) {
    Preconditions.checkNotNull(outputStream);
    try (ObjectOutputStream out = new ObjectOutputStream(outputStream)) {
      out.writeObject(obj);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * <p>Serializes an {@code Object} to a byte array for
   * storage/serialization.</p>
   *
   * @param obj the object to serialize to bytes
   * @return a byte[] with the converted Serializable
   */
  public static byte[] serialize(Serializable obj) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
    serialize(obj, baos);
    return baos.toByteArray();
  }

  // Deserialize
  //-----------------------------------------------------------------------

  /**
   * <p>Deserializes an {@code Object} from the specified stream.</p>
   * <p/>
   * <p>The stream will be closed once the object is written. This
   * avoids the need for a finally clause, and maybe also exception
   * handling, in the application code.</p>
   * <p/>
   * <p>The stream passed in is not buffered internally within this method.
   * This is the responsibility of your application if desired.</p>
   *
   * @param inputStream the serialized object input stream, must not be null
   * @return the deserialized object
   * @throws IllegalArgumentException if {@code inputStream} is {@code null}
   */
  public static Object deserialize(InputStream inputStream) {
    Preconditions.checkNotNull(inputStream);
    try (ObjectInputStream in = new ObjectInputStream(inputStream)) {
      return in.readObject();
    } catch (ClassNotFoundException | IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * <p>Deserializes a single {@code Object} from an array of bytes.</p>
   *
   * @param objectData the serialized object, must not be null
   * @return the deserialized object
   * @throws IllegalArgumentException if {@code objectData} is {@code null}
   */
  public static Object deserialize(byte[] objectData) {
    Preconditions.checkNotNull(objectData);
    return deserialize(new ByteArrayInputStream(objectData));
  }

}
