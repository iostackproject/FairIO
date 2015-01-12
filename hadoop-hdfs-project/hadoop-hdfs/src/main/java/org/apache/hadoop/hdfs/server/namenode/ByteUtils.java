package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by DEIM on 16/10/14.
 */
public class ByteUtils {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  public static byte[] floatToByte(Float f) {
    return String.valueOf(f).getBytes();
  }

  public static float bytesToFloat(byte[] b) {
    return new Float(new String(b));
  }
}
