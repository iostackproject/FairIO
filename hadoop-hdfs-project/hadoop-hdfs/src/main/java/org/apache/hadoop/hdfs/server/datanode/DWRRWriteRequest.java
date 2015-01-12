package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;

import java.nio.ByteBuffer;

/**
 * Created by DEIM on 8/09/14.
 */
public class DWRRWriteRequest {

  private boolean syncBlock;
  private boolean lastPacketInBlock;
  private int len;
  private long offsetInBlock;
  private byte[] checksumArray;
  private PacketHeader header;
  private int checksumLen;
  private ByteBuffer checksumBuf;
  private byte[] array;
  private int startByteToDisk;
  private int numBytesToDisk;

  public DWRRWriteRequest(byte[] array, int startByteToDisk, int numBytesToDisk, ByteBuffer checksumBuf, byte[] checksumArray, int checksumLen, int len, long offsetInBlock, boolean syncBlock, boolean lastPacketInBlock) {
    this.array = array;
    this.startByteToDisk = startByteToDisk;
    this.numBytesToDisk = numBytesToDisk;
    this.checksumBuf = checksumBuf;
    this.checksumLen = checksumLen;
    this.checksumArray = checksumArray;
    this.len = len;
    this.offsetInBlock = offsetInBlock;
    this.lastPacketInBlock = lastPacketInBlock;
    this.syncBlock = syncBlock;
  }

  public boolean getSyncBlock() {
    return syncBlock;
  }

  public boolean getLastPacketInBlock() {
    return lastPacketInBlock;
  }

  public int getLen() {
    return len;
  }

  public long getOffsetInBlock() {
    return offsetInBlock;
  }

  public byte[] getArray() {
    return array;
  }

  public int getStartByteToDisk() {
    return startByteToDisk;
  }

  public int getNumBytesToDisk() {
    return numBytesToDisk;
  }

  public ByteBuffer getChecksumBuf() {
    return checksumBuf;
  }

  public PacketHeader getHeader() {
    return header;
  }

  public int getChecksumLen() {
    return checksumLen;
  }

  public byte[] getChecksumArray() {
    return checksumArray;
  }

  public void clear() {
    syncBlock = false;
    lastPacketInBlock = false;
    len = 0;
    offsetInBlock = 0;
    checksumArray = null;
    header = null;
    checksumLen = 0;
    checksumBuf = null;
    array = null;
    startByteToDisk = 0;
    numBytesToDisk = 0;

  }
}
