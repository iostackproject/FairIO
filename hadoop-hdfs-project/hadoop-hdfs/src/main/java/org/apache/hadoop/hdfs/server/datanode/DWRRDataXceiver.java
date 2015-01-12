/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor.InvalidMagicNumberException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.*;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsUnsupportedException;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsVersionException;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry.NewShmInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Queue;

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.fromProto;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.*;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
import static org.apache.hadoop.util.Time.now;


/**
 * Thread for processing incoming/outgoing data stream.
 */
public class DWRRDataXceiver extends Receiver implements Runnable {
  public static final Log LOG = LogFactory.getLog(DWRRDataXceiver.class);
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  private Peer peer;
  private final String remoteAddress; // address of remote side
  private final String localAddress;  // local address of this daemon
  private final DataNode datanode;
  private final DNConf dnConf;
  private final DataXceiverServer DataXceiverServer;
  private final boolean connectToDnViaHostname;
  private long opStartTime; //the start time of receiving an Op
  private final InputStream socketIn;
  private OutputStream socketOut;

  private DWRRManager dwrrmanager;
  private long classId;
  private DWRRBlockReceiver blockReceiver = null;
  private Op op;
  private String storageUuid;
  private BlockConstructionStage stage;
  private ExtendedBlock block;
  private long latestGenerationStamp;
  private long minBytesRcvd;
  private DataTransferProtos.OpReadBlockProto proto;
  private DataOutputStream mirrorOut;
  private DataInputStream mirrorIn;
  private DataOutputStream replyOut;

  public int getOpsQueded() {
    return opsQueded;
  }

  private int opsQueded;

  private Queue<DWRRRequestObject> listPacketReceiver = null;

  private boolean isDatanode;
  private boolean isClient;
  private boolean isTransfer;

  /**
   * Client Name used in previous operation. Not available on first request
   * on the socket.
   */
  private String previousOpClientName;
  
  public static DWRRDataXceiver create(Peer peer, DataNode dn,
                                       DataXceiverServer DataXceiverServer, DWRRManager dwrrmanager) throws IOException {
    return new DWRRDataXceiver(peer, dn, DataXceiverServer, dwrrmanager);
  }
  
  private DWRRDataXceiver(Peer peer, DataNode datanode,
                          DataXceiverServer DataXceiverServer, DWRRManager dwrrmanager) throws IOException {

    this.peer = peer;
    this.dnConf = datanode.getDnConf();
    this.socketIn = peer.getInputStream();
    this.socketOut = peer.getOutputStream();
    this.datanode = datanode;
    this.DataXceiverServer = DataXceiverServer;
    this.connectToDnViaHostname = datanode.getDnConf().connectToDnViaHostname;
    this.dwrrmanager = dwrrmanager;
    remoteAddress = peer.getRemoteAddressString();
    localAddress = peer.getLocalAddressString();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Number of active connections is: "
          + datanode.getXceiverCount());
    }
    LOG.info("CAMAMILLA DWRRDataXceiver.constructor time="+now());      // TODO TODO log
  }

  /**
   * Update the current thread's name to contain the current status.
   * Use this only after this receiver has started on its thread, i.e.,
   * outside the constructor.
   */
  private void updateCurrentThreadName(String status) {
    StringBuilder sb = new StringBuilder();
    sb.append("DWRRDataXceiver for client ");
    if (previousOpClientName != null) {
      sb.append(previousOpClientName).append(" at ");
    }
    sb.append(remoteAddress);
    if (status != null) {
      sb.append(" [").append(status).append("]");
    }
    Thread.currentThread().setName(sb.toString());
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}
  
  private OutputStream getOutputStream() {
    return socketOut;
  }

  /**
   * Process op by the corresponding method.
   */
  public synchronized void makeOp(Op op) throws IOException {
    LOG.info("CAMAMILLA "+classId+" makeOp " + op.code);      // TODO TODO log
    switch (op) {
      case READ_BLOCK:
        makeReadBlock(proto);
        break;
      case WRITE_BLOCK:
        makeWriteBlock();
        break;
//			case REPLACE_BLOCK:
//				opReplaceBlock();
//				break;
//			case COPY_BLOCK:
//				opCopyBlock();
//				break;
//			case BLOCK_CHECKSUM:
//				opBlockChecksum();
//				break;
//			case TRANSFER_BLOCK:
//				opTransferBlock();
//				break;
//			case REQUEST_SHORT_CIRCUIT_FDS:
//				opRequestShortCircuitFds();
//				break;
//			case RELEASE_SHORT_CIRCUIT_FDS:
//				opReleaseShortCircuitFds();
//				break;
//			case REQUEST_SHORT_CIRCUIT_SHM:
//				opRequestShortCircuitShm();
//				break;
      default:
        LOG.error("CAMAMILLA "+classId+" ERROR makeOp " + op.code);      // TODO TODO log
        throw new IOException("Unknown op " + op + " in makeOp");
    }

    finalizeDXC();
  }


  /** Receive OP_READ_BLOCK */
  private void opReadBlock() throws IOException {
    LOG.info("CAMAMILLA DWRRDataXceiver.opReadBlock queue init time="+now());      // TODO TODO log
    proto = DataTransferProtos.OpReadBlockProto.parseFrom(vintPrefixed(in));
    classId = proto.getHeader().getBaseHeader().getBlock().getClassId();
    LOG.info("CAMAMILLA "+classId+" ENQUEUE READ INIT");        // TODO TODO log

    DWRRRequestObject newReq = new DWRRRequestObject(this, classId, op, proto.getLen());
    dwrrmanager.addOp(newReq, classId);

    LOG.info("CAMAMILLA DWRRDataXceiver.opReadBlock queue end time="+now());      // TODO TODO log
    LOG.info("CAMAMILLA "+classId+" ENQUEUE READ FINALIZED");        // TODO TODO log
  }

  private void makeReadBlock(final DataTransferProtos.OpReadBlockProto protoc) {
    LOG.info("CAMAMILLA "+classId+" READ INIT");      // TODO TODO log
    LOG.info("CAMAMILLA DWRRDataXceiver.makeReadBlock init time="+now());      // TODO TODO log
    try {
      readBlock(PBHelper.convert(protoc.getHeader().getBaseHeader().getBlock()),
        PBHelper.convert(protoc.getHeader().getBaseHeader().getToken()),
        protoc.getHeader().getClientName(),
        protoc.getOffset(),
        protoc.getLen(),
        protoc.getSendChecksums(),
        (protoc.hasCachingStrategy() ?
          getCachingStrategy(protoc.getCachingStrategy()) :
          CachingStrategy.newDefaultStrategy()));
      LOG.info("CAMAMILLA "+classId+" READ FINALIZED");      // TODO TODO log
    } catch (IOException e) {
      LOG.error("CAMAMILLA "+classId+" error");      // TODO TODO log
      e.printStackTrace();
    }
    LOG.info("CAMAMILLA DWRRDataXceiver.makeReadBlock end time="+now());      // TODO TODO log
  }

  /** Receive OP_WRITE_BLOCK */
  private void opWriteBlock() throws IOException {
    LOG.info("CAMAMILLA DWRRDataXceiver.opWriteBlock queue init time="+now());      // TODO TODO log
    DataTransferProtos.OpWriteBlockProto proto = DataTransferProtos.OpWriteBlockProto.parseFrom(vintPrefixed(in));
    classId = proto.getHeader().getBaseHeader().getBlock().getClassId();
    LOG.info("CAMAMILLA "+classId+" ENQUEUE WRITE INIT");        // TODO TODO log
    try {
      writeBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        PBHelper.convert(proto.getTargetsList()),
        PBHelper.convert(proto.getSource()),
        BlockConstructionStage.valueOf(proto.getStage().name()),
        proto.getPipelineSize(),
        proto.getMinBytesRcvd(), proto.getMaxBytesRcvd(),
        proto.getLatestGenerationStamp(),
        fromProto(proto.getRequestedChecksum()),
        (proto.hasCachingStrategy() ?
          getCachingStrategy(proto.getCachingStrategy()) :
          CachingStrategy.newDefaultStrategy()));
    } catch (IOException e) {
      LOG.info("CAMAMILLA "+classId+" error");      // TODO TODO log
      e.printStackTrace();
    }
    LOG.info("CAMAMILLA DWRRDataXceiver.opWriteBlock queue end time="+now());      // TODO TODO log
    LOG.info("CAMAMILLA "+classId+" ENQUEUE WRITE FINALIZED");        // TODO TODO log
  }

  private void makeWriteBlock () {
    LOG.info("CAMAMILLA "+classId+" WRITE INIT");      // TODO TODO log
    LOG.info("CAMAMILLA DWRRDataXceiver.makeWriteBlock init time="+now());      // TODO TODO log
    finalizeWriteBlock();
    closeStreams();
    LOG.info("CAMAMILLA "+classId+" WRITE FINALIZED");      // TODO TODO log
    LOG.info("CAMAMILLA DWRRDataXceiver.makeWriteBlock end time="+now());      // TODO TODO log
  }
  /**
   * Read/write data from/to the DataXceiverServer.
   */
  @Override
  public void run() {
    int opsProcessed = 0;
    opsQueded = 0;
    LOG.info("CAMAMILLA DWRRDataXceiver.run time="+now());      // TODO TODO log
    op = null;
    try {
      DataXceiverServer.addPeer(peer, Thread.currentThread());
      peer.setWriteTimeout(datanode.getDnConf().socketWriteTimeout);
      InputStream input = socketIn;
      if ((!peer.hasSecureChannel()) && dnConf.encryptDataTransfer &&
          !dnConf.trustedChannelResolver.isTrusted(getClientAddress(peer))){
        IOStreamPair encryptedStreams = null;
        try {
          encryptedStreams = DataTransferEncryptor.getEncryptedStreams(socketOut,
              socketIn, datanode.blockPoolTokenSecretManager,
              dnConf.encryptionAlgorithm);
        } catch (InvalidMagicNumberException imne) {
          LOG.info("Failed to read expected encryption handshake from client " +
              "at " + peer.getRemoteAddressString() + ". Perhaps the client " +
              "is running an older version of Hadoop which does not support " +
              "encryption");
          return;
        }
        input = encryptedStreams.in;
        socketOut = encryptedStreams.out;
      }
      input = new BufferedInputStream(input, HdfsConstants.SMALL_BUFFER_SIZE);
      
      super.initialize(new DataInputStream(input));
      
      // We process requests in a loop, and stay around for a short timeout.
      // This optimistic behaviour allows the other end to reuse connections.
      // Setting keepalive timeout to 0 disable this behavior.
      do {
        updateCurrentThreadName("Waiting for operation #" + (opsProcessed + 1));

        try {
          if (opsProcessed != 0) {
            assert dnConf.socketKeepaliveTimeout > 0;
            peer.setReadTimeout(dnConf.socketKeepaliveTimeout);
          } else {
            peer.setReadTimeout(dnConf.socketTimeout);
          }
          op = readOp();
        } catch (InterruptedIOException ignored) {
          // Time out while we wait for client rpc
          break;
        } catch (IOException err) {
          // Since we optimistically expect the next op, it's quite normal to get EOF here.
          if (opsProcessed > 0 &&
              (err instanceof EOFException || err instanceof ClosedChannelException)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cached " + peer + " closing after " + opsProcessed + " ops");
            }
          } else {
            throw err;
          }
          break;
        }

        // restore normal timeout
        if (opsProcessed != 0) {
          peer.setReadTimeout(dnConf.socketTimeout);
        }

        opStartTime = now();
        processOp();
        ++opsProcessed;
      } while ((peer != null) &&
          (!peer.isClosed() && dnConf.socketKeepaliveTimeout > 0));
      opsQueded = opsProcessed;
    } catch (Throwable t) {
      String s = datanode.getDisplayName() + ":DWRRDataXceiver error processing "
          + ((op == null) ? "unknown" : op.name()) + " operation "
          + " src: " + remoteAddress + " dst: " + localAddress;
      if (op == Op.WRITE_BLOCK && t instanceof ReplicaAlreadyExistsException) {
        // For WRITE_BLOCK, it is okay if the replica already exists since
        // client and replication may write the same block to the same datanode
        // at the same time.
        if (LOG.isTraceEnabled()) {
          LOG.trace(s, t);
        } else {
          LOG.info(s + "; " + t);
        }
      } else {
        LOG.error(s, t);
      }
      LOG.error("CAMAMILLA "+classId+" finalize DWRRDataXceiver per error");      // TODO TODO log
      finalizeDXC();
    }
  }

  public void finalizeDXC() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(datanode.getDisplayName() + ":Number of active connections is: "
        + datanode.getXceiverCount());
    }
    updateCurrentThreadName("Cleaning up");
    if (peer != null) {
      DataXceiverServer.closePeer(peer);
      IOUtils.closeStream(in);
    }
  }

  private void processOp()  throws IOException {
    switch(op) {
      case READ_BLOCK:
        LOG.info("CAMAMILLA processOp READ");      // TODO TODO log
        opReadBlock();
        break;
      case WRITE_BLOCK:
        LOG.info("CAMAMILLA processOp WRITE");      // TODO TODO log
        opWriteBlock();
        break;
      case REPLACE_BLOCK:
        LOG.info("CAMAMILLA processOp REPLACE");      // TODO TODO log
        processOp(op);
        break;
      case COPY_BLOCK:
        LOG.info("CAMAMILLA processOp COPY");      // TODO TODO log
        processOp(op);
        break;
      case BLOCK_CHECKSUM:
        LOG.info("CAMAMILLA processOp CHECKSUM");      // TODO TODO log
        processOp(op);
        break;
      case TRANSFER_BLOCK:
        LOG.info("CAMAMILLA processOp TRANSFER");      // TODO TODO log
        processOp(op);
        break;
      case REQUEST_SHORT_CIRCUIT_FDS:
        LOG.info("CAMAMILLA processOp REQUEST");      // TODO TODO log
        processOp(op);
        break;
      case RELEASE_SHORT_CIRCUIT_FDS:
        LOG.info("CAMAMILLA processOp RELEASE");      // TODO TODO log
        processOp(op);
        break;
      case REQUEST_SHORT_CIRCUIT_SHM:
        LOG.info("CAMAMILLA processOp REQUEST2");      // TODO TODO log
        processOp(op);
        break;
      default:
        throw new IOException("Unknown op " + op + " in data stream");
    }
  }

  /**
   * Returns InetAddress from peer
   * The getRemoteAddressString is the form  /ip-address:port
   * The ip-address is extracted from peer and InetAddress is formed
   * @param peer
   * @return
   * @throws UnknownHostException
   */
  private static InetAddress getClientAddress(Peer peer) {
    return InetAddresses.forString(
        peer.getRemoteAddressString().split(":")[0].substring(1));
  }

  @Override
  public void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> token,
      SlotId slotId, int maxVersion) throws IOException {
    updateCurrentThreadName("Passing file descriptors for block " + blk);
    BlockOpResponseProto.Builder bld = BlockOpResponseProto.newBuilder();
    FileInputStream fis[] = null;
    try {
      if (peer.getDomainSocket() == null) {
        throw new IOException("You cannot pass file descriptors over " +
            "anything but a UNIX domain socket.");
      }
      if (slotId != null) {
        boolean isCached = datanode.data.
            isCached(blk.getBlockPoolId(), blk.getBlockId());
        datanode.shortCircuitRegistry.registerSlot(
            ExtendedBlockId.fromExtendedBlock(blk), slotId, isCached);
      }
      try {
        fis = datanode.requestShortCircuitFdsForRead(blk, token, maxVersion);
      } finally {
        if ((fis == null) && (slotId != null)) {
          datanode.shortCircuitRegistry.unregisterSlot(slotId);
        }
      }
      bld.setStatus(SUCCESS);
      bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
    } catch (ShortCircuitFdsVersionException e) {
      bld.setStatus(ERROR_UNSUPPORTED);
      bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
      bld.setMessage(e.getMessage());
    } catch (ShortCircuitFdsUnsupportedException e) {
      bld.setStatus(ERROR_UNSUPPORTED);
      bld.setMessage(e.getMessage());
    } catch (InvalidToken e) {
      bld.setStatus(ERROR_ACCESS_TOKEN);
      bld.setMessage(e.getMessage());
    } catch (IOException e) {
      bld.setStatus(ERROR);
      bld.setMessage(e.getMessage());
    }
    try {
      bld.build().writeDelimitedTo(socketOut);
      if (fis != null) {
        FileDescriptor fds[] = new FileDescriptor[fis.length];
        for (int i = 0; i < fds.length; i++) {
          fds[i] = fis[i].getFD();
        }
        byte buf[] = new byte[] { (byte)0 };
        peer.getDomainSocket().
          sendFileDescriptors(fds, buf, 0, buf.length);
      }
    } finally {
      if (ClientTraceLog.isInfoEnabled()) {
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(blk
            .getBlockPoolId());
        BlockSender.ClientTraceLog.info(String.format(
            "src: 127.0.0.1, dest: 127.0.0.1, op: REQUEST_SHORT_CIRCUIT_FDS," +
            " blockid: %s, srvID: %s, success: %b",
            blk.getBlockId(), dnR.getDatanodeUuid(), (fis != null)
          ));
      }
      if (fis != null) {
        IOUtils.cleanup(LOG, fis);
      }
    }
  }

  @Override
  public void releaseShortCircuitFds(SlotId slotId) throws IOException {
    boolean success = false;
    try {
      String error;
      Status status;
      try {
        datanode.shortCircuitRegistry.unregisterSlot(slotId);
        error = null;
        status = Status.SUCCESS;
      } catch (UnsupportedOperationException e) {
        error = "unsupported operation";
        status = Status.ERROR_UNSUPPORTED;
      } catch (Throwable e) {
        error = e.getMessage();
        status = Status.ERROR_INVALID;
      }
      ReleaseShortCircuitAccessResponseProto.Builder bld =
          ReleaseShortCircuitAccessResponseProto.newBuilder();
      bld.setStatus(status);
      if (error != null) {
        bld.setError(error);
      }
      bld.build().writeDelimitedTo(socketOut);
      success = true;
    } finally {
      if (ClientTraceLog.isInfoEnabled()) {
        BlockSender.ClientTraceLog.info(String.format(
            "src: 127.0.0.1, dest: 127.0.0.1, op: RELEASE_SHORT_CIRCUIT_FDS," +
            " shmId: %016x%016x, slotIdx: %d, srvID: %s, success: %b",
            slotId.getShmId().getHi(), slotId.getShmId().getLo(),
            slotId.getSlotIdx(), datanode.getDatanodeUuid(), success));
      }
    }
  }

  private void sendShmErrorResponse(Status status, String error)
      throws IOException {
    ShortCircuitShmResponseProto.newBuilder().setStatus(status).
        setError(error).build().writeDelimitedTo(socketOut);
  }

  private void sendShmSuccessResponse(DomainSocket sock, NewShmInfo shmInfo)
      throws IOException {
    ShortCircuitShmResponseProto.newBuilder().setStatus(SUCCESS).
        setId(PBHelper.convert(shmInfo.shmId)).build().
        writeDelimitedTo(socketOut);
    // Send the file descriptor for the shared memory segment.
    byte buf[] = new byte[] { (byte)0 };
    FileDescriptor shmFdArray[] =
        new FileDescriptor[] { shmInfo.stream.getFD() };
    sock.sendFileDescriptors(shmFdArray, buf, 0, buf.length);
  }

  @Override
  public void requestShortCircuitShm(String clientName) throws IOException {
    NewShmInfo shmInfo = null;
    boolean success = false;
    DomainSocket sock = peer.getDomainSocket();
    try {
      if (sock == null) {
        sendShmErrorResponse(ERROR_INVALID, "Bad request from " +
            peer + ": must request a shared " +
            "memory segment over a UNIX domain socket.");
        return;
      }
      try {
        shmInfo = datanode.shortCircuitRegistry.
            createNewMemorySegment(clientName, sock);
        // After calling #{ShortCircuitRegistry#createNewMemorySegment}, the
        // socket is managed by the DomainSocketWatcher, not the DWRRDataXceiver.
        releaseSocket();
      } catch (UnsupportedOperationException e) {
        sendShmErrorResponse(ERROR_UNSUPPORTED, 
            "This datanode has not been configured to support " +
            "short-circuit shared memory segments.");
        return;
      } catch (IOException e) {
        sendShmErrorResponse(ERROR,
            "Failed to create shared file descriptor: " + e.getMessage());
        return;
      }
      sendShmSuccessResponse(sock, shmInfo);
      success = true;
    } finally {
      if (ClientTraceLog.isInfoEnabled()) {
        if (success) {
          BlockSender.ClientTraceLog.info(String.format(
              "cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, " +
              "op: REQUEST_SHORT_CIRCUIT_SHM," +
              " shmId: %016x%016x, srvID: %s, success: true",
              clientName, shmInfo.shmId.getHi(), shmInfo.shmId.getLo(),
              datanode.getDatanodeUuid()));
        } else {
          BlockSender.ClientTraceLog.info(String.format(
              "cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, " +
              "op: REQUEST_SHORT_CIRCUIT_SHM, " +
              "shmId: n/a, srvID: %s, success: false",
              clientName, datanode.getDatanodeUuid()));
        }
      }
      if ((!success) && (peer == null)) {
        // If we failed to pass the shared memory segment to the client,
        // close the UNIX domain socket now.  This will trigger the 
        // DomainSocketWatcher callback, cleaning up the segment.
        IOUtils.cleanup(null, sock);
      }
      IOUtils.cleanup(null, shmInfo);
    }
  }

  void releaseSocket() {
    DataXceiverServer.releasePeer(peer);
    peer = null;
  }

  @Override
  public void readBlock(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum,
      final CachingStrategy cachingStrategy) throws IOException {
    previousOpClientName = clientName;

    OutputStream baseStream = getOutputStream();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        baseStream, HdfsConstants.SMALL_BUFFER_SIZE));
    checkAccess(out, true, block, blockToken,
        Op.READ_BLOCK, BlockTokenSecretManager.AccessMode.READ);
  
    // send the block
    BlockSender blockSender = null;
    DatanodeRegistration dnR = 
      datanode.getDNRegistrationForBP(block.getBlockPoolId());
    final String clientTraceFmt =
      clientName.length() > 0 && ClientTraceLog.isInfoEnabled()
        ? String.format(DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress,
            "%d", "HDFS_READ", clientName, "%d",
            dnR.getDatanodeUuid(), block, "%d")
        : dnR + " Served block " + block + " to " +
            remoteAddress;

    updateCurrentThreadName("Sending block " + block);
    try {
      try {
        // TODO TODO lectura de bloc aqui!
        blockSender = new BlockSender(block, blockOffset, length,
            true, false, sendChecksum, datanode, clientTraceFmt,
            cachingStrategy);
      } catch(IOException e) {
        String msg = "opReadBlock " + block + " received exception " + e; 
        LOG.info(msg);
        sendResponse(ERROR, msg);
        throw e;
      }
      // TODO TODO TODO!!! en aquest punt hauriem de poder avisar que es pot procedir amb les lecutres a disc, i que llanci la seguent
      readBlockFinalize(baseStream, out, blockSender);

    } catch (IOException ioe) {
			/* What exactly should we do here?
       * Earlier version shutdown() datanode if there is disk error.
       */
      LOG.warn(dnR + ":Got exception while serving " + block + " to "
        + remoteAddress, ioe);
      throw ioe;
    } finally {
      IOUtils.closeStream(blockSender);
    }

    //update metrics
    datanode.metrics.addReadBlockOp(elapsed());
    datanode.metrics.incrReadsFromClient(peer.isLocal());
  }

  private void readBlockFinalize(OutputStream baseStream, DataOutputStream out, BlockSender blockSender) throws IOException {
    // send op status
    writeSuccessWithChecksumInfo(blockSender, new DataOutputStream(getOutputStream()));
    long read = blockSender.sendBlock(out, baseStream, null); // send data
    if (blockSender.didSendEntireByteRange()) {
      // If we sent the entire range, then we should expect the client
      // to respond with a Status enum.
      try {
        ClientReadStatusProto stat = ClientReadStatusProto.parseFrom(
          PBHelper.vintPrefixed(in));
        if (!stat.hasStatus()) {
          LOG.warn("Client " + peer.getRemoteAddressString() +
            " did not send a valid status code after reading. " +
            "Will close connection.");
          IOUtils.closeStream(out);
        }
      } catch (IOException ioe) {
        LOG.debug("Error reading client status response. Will close connection.", ioe);
        IOUtils.closeStream(out);
      }
    } else {
      IOUtils.closeStream(out);
    }
    datanode.metrics.incrBytesRead((int) read);
    datanode.metrics.incrBlocksRead();
  }

  @Override
  public void writeBlock(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientname,
      final DatanodeInfo[] targets,
      final DatanodeInfo srcDataNode,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy) throws IOException {

    previousOpClientName = clientname;
    updateCurrentThreadName("Receiving block " + block);
    isDatanode = clientname.length() == 0;
    isClient = !isDatanode;
    isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
        || stage == BlockConstructionStage.TRANSFER_FINALIZED;

    this.stage = stage;
    this.block = block;
    this.latestGenerationStamp = latestGenerationStamp;
    this.minBytesRcvd = minBytesRcvd;
    // check single target for transfer-RBW/Finalized
    if (isTransfer && targets.length > 0) {
      throw new IOException(stage + " does not support multiple targets "
          + Arrays.asList(targets));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("opWriteBlock: stage=" + stage + ", clientname=" + clientname 
      		+ "\n  block  =" + block + ", newGs=" + latestGenerationStamp
      		+ ", bytesRcvd=[" + minBytesRcvd + ", " + maxBytesRcvd + "]"
          + "\n  targets=" + Arrays.asList(targets)
          + "; pipelineSize=" + pipelineSize + ", srcDataNode=" + srcDataNode
          );
      LOG.debug("isDatanode=" + isDatanode
          + ", isClient=" + isClient
          + ", isTransfer=" + isTransfer);
      LOG.debug("writeBlock receive buf size " + peer.getReceiveBufferSize() +
                " tcp no delay " + peer.getTcpNoDelay());
    }

    // We later mutate block's generation stamp and length, but we need to
    // forward the original version of the block to downstream mirrors, so
    // make a copy here.
    final ExtendedBlock originalBlock = new ExtendedBlock(block);
    block.setNumBytes(DataXceiverServer.estimateBlockSize);
    LOG.info("Receiving " + block + " src: " + remoteAddress + " dest: "
        + localAddress);

    // reply to upstream datanode or client 
    replyOut = new DataOutputStream(
        new BufferedOutputStream(
            getOutputStream(),
            HdfsConstants.SMALL_BUFFER_SIZE));
    checkAccess(replyOut, isClient, block, blockToken,
        Op.WRITE_BLOCK, BlockTokenSecretManager.AccessMode.WRITE);

    mirrorOut = null;  // stream to next target
    mirrorIn = null;    // reply from next target
    Socket mirrorSock = null;           // socket to next target
    blockReceiver = null; // responsible for data handling
    String mirrorNode = null;           // the name:port of next target
    String firstBadLink = "";           // first datanode that failed in connection setup
    Status mirrorInStatus = SUCCESS;
    try {
      if (isDatanode || 
          stage != BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        // open a block receiver
        blockReceiver = new DWRRBlockReceiver(block, in,
            peer.getRemoteAddressString(),
            peer.getLocalAddressString(),
            stage, latestGenerationStamp, minBytesRcvd, maxBytesRcvd,
            clientname, srcDataNode, datanode, requestedChecksum,
            cachingStrategy);
        storageUuid = blockReceiver.getStorageUuid();
      } else {
        storageUuid = datanode.data.recoverClose(
            block, latestGenerationStamp, minBytesRcvd);
      }

      //
      // Connect to downstream machine, if appropriate
      //
      if (targets.length > 0) {
        InetSocketAddress mirrorTarget = null;
        // Connect to backup machine
        mirrorNode = targets[0].getXferAddr(connectToDnViaHostname);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to datanode " + mirrorNode);
        }
        mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
        mirrorSock = datanode.newSocket();
        try {
          int timeoutValue = dnConf.socketTimeout
              + (HdfsServerConstants.READ_TIMEOUT_EXTENSION * targets.length);
          int writeTimeout = dnConf.socketWriteTimeout + 
                      (HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * targets.length);
          NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);
          mirrorSock.setSoTimeout(timeoutValue);
          mirrorSock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
          
          OutputStream unbufMirrorOut = NetUtils.getOutputStream(mirrorSock,
              writeTimeout);
          InputStream unbufMirrorIn = NetUtils.getInputStream(mirrorSock);
          if (dnConf.encryptDataTransfer &&
              !dnConf.trustedChannelResolver.isTrusted(mirrorSock.getInetAddress())) {
            IOStreamPair encryptedStreams =
                DataTransferEncryptor.getEncryptedStreams(
                    unbufMirrorOut, unbufMirrorIn,
                    datanode.blockPoolTokenSecretManager
                        .generateDataEncryptionKey(block.getBlockPoolId()));
            
            unbufMirrorOut = encryptedStreams.out;
            unbufMirrorIn = encryptedStreams.in;
          }
          mirrorOut = new DataOutputStream(new BufferedOutputStream(unbufMirrorOut,
              HdfsConstants.SMALL_BUFFER_SIZE));
          mirrorIn = new DataInputStream(unbufMirrorIn);

          new Sender(mirrorOut).writeBlock(originalBlock, blockToken,
              clientname, targets, srcDataNode, stage, pipelineSize,
              minBytesRcvd, maxBytesRcvd, latestGenerationStamp, requestedChecksum,
              cachingStrategy);

          mirrorOut.flush();

          // read connect ack (only for clients, not for replication req)
          if (isClient) {
            BlockOpResponseProto connectAck =
              BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(mirrorIn));
            mirrorInStatus = connectAck.getStatus();
            firstBadLink = connectAck.getFirstBadLink();
            if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
              LOG.info("Datanode " + targets.length +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
            }
          }

        } catch (IOException e) {
          if (isClient) {
            BlockOpResponseProto.newBuilder()
              .setStatus(ERROR)
               // NB: Unconditionally using the xfer addr w/o hostname
              .setFirstBadLink(targets[0].getXferAddr())
              .build()
              .writeDelimitedTo(replyOut);
            replyOut.flush();
          }
          IOUtils.closeStream(mirrorOut);
          mirrorOut = null;
          IOUtils.closeStream(mirrorIn);
          mirrorIn = null;
          IOUtils.closeSocket(mirrorSock);
          mirrorSock = null;
          if (isClient) {
            LOG.error(datanode + ":Exception transfering block " +
                      block + " to mirror " + mirrorNode + ": " + e);
            throw e;
          } else {
            LOG.info(datanode + ":Exception transfering " +
                     block + " to mirror " + mirrorNode +
                     "- continuing without the mirror", e);
          }
        }
      }

      // send connect-ack to source for clients and not transfer-RBW/Finalized
      if (isClient && !isTransfer) {
        if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
          LOG.info("Datanode " + targets.length +
                   " forwarding connect ack to upstream firstbadlink is " +
                   firstBadLink);
        }
        BlockOpResponseProto.newBuilder()
          .setStatus(mirrorInStatus)
          .setFirstBadLink(firstBadLink)
          .build()
          .writeDelimitedTo(replyOut);
        replyOut.flush();
      }

      // receive the block and mirror to the next target
      if (blockReceiver != null) {
        LOG.info("CAMAMILLA " + classId + " receive the block and mirror to the next target");      // TODO TODO log entra
        String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
        int len = blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
          mirrorAddr, null, targets);

        DWRRRequestObject newReq = new DWRRRequestObject(this, classId, op, len);
        dwrrmanager.addOp(newReq, classId);
        LOG.info("CAMAMILLA " + classId + " encuada peticio WRITE ");      // TODO TODO log
      }
    } catch (IOException ioe) {
      LOG.info("opWriteBlock " + block + " received exception " + ioe);
      throw ioe;
    } finally {
      // close all opened streams
      IOUtils.closeSocket(mirrorSock);
    }
  }

  private void closeStreams() {
    IOUtils.closeStream(mirrorOut);
    IOUtils.closeStream(mirrorIn);
    IOUtils.closeStream(replyOut);
  }

  private void finalizeWriteBlock() {
    try {
      DataOutputStream replyOut = new DataOutputStream(
        new BufferedOutputStream(
          getOutputStream(),
          HdfsConstants.SMALL_BUFFER_SIZE));

      blockReceiver.finalizeReceiveBlock();

      // send close-ack for transfer-RBW/Finalized
      if (isTransfer) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("TRANSFER: send close-ack");
        }
        writeResponse(SUCCESS, null, replyOut);
      }

      // update its generation stamp
      if (isClient &&
        stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        block.setGenerationStamp(latestGenerationStamp);
        block.setNumBytes(minBytesRcvd);
      }

      // if this write is for a replication request or recovering
      // a failed close for client, then confirm block. For other client-writes,
      // the block is finalized in the PacketResponder.
      if (isDatanode ||
        stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        datanode.closeBlock(block, DataNode.EMPTY_DEL_HINT, storageUuid);
        LOG.info("Received " + block + " src: " + remoteAddress + " dest: "
          + localAddress + " of size " + block.getNumBytes());
      }
    } catch (IOException e) {
      LOG.error("CAMAMILLA "+classId+" exception finalizeWriteBlock "+e);      // TODO TODO log
    } finally {
      IOUtils.closeStream(blockReceiver);
    }

    //update metrics
    datanode.metrics.addWriteBlockOp(elapsed());
    datanode.metrics.incrWritesFromClient(peer.isLocal());
  }

  @Override
  public void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets) throws IOException {
    checkAccess(socketOut, true, blk, blockToken,
        Op.TRANSFER_BLOCK, BlockTokenSecretManager.AccessMode.COPY);
    previousOpClientName = clientName;
    updateCurrentThreadName(Op.TRANSFER_BLOCK + " " + blk);

    final DataOutputStream out = new DataOutputStream(
        getOutputStream());
    try {
      datanode.transferReplicaForPipelineRecovery(blk, targets, clientName);
      writeResponse(Status.SUCCESS, null, out);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  private MD5Hash calcPartialBlockChecksum(ExtendedBlock block,
      long requestLength, DataChecksum checksum, DataInputStream checksumIn)
      throws IOException {
    final int bytesPerCRC = checksum.getBytesPerChecksum();
    final int csize = checksum.getChecksumSize();
    final byte[] buffer = new byte[4*1024];
    MessageDigest digester = MD5Hash.getDigester();

    long remaining = requestLength / bytesPerCRC * csize;
    for (int toDigest = 0; remaining > 0; remaining -= toDigest) {
      toDigest = checksumIn.read(buffer, 0,
          (int) Math.min(remaining, buffer.length));
      if (toDigest < 0) {
        break;
      }
      digester.update(buffer, 0, toDigest);
    }
    
    int partialLength = (int) (requestLength % bytesPerCRC);
    if (partialLength > 0) {
      byte[] buf = new byte[partialLength];
      final InputStream blockIn = datanode.data.getBlockInputStream(block,
          requestLength - partialLength);
      try {
        // Get the CRC of the partialLength.
        IOUtils.readFully(blockIn, buf, 0, partialLength);
      } finally {
        IOUtils.closeStream(blockIn);
      }
      checksum.update(buf, 0, partialLength);
      byte[] partialCrc = new byte[csize];
      checksum.writeValue(partialCrc, 0, true);
      digester.update(partialCrc);
    }
    return new MD5Hash(digester.digest());
  }

  @Override
  public void blockChecksum(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    final DataOutputStream out = new DataOutputStream(
        getOutputStream());
    checkAccess(out, true, block, blockToken,
        Op.BLOCK_CHECKSUM, BlockTokenSecretManager.AccessMode.READ);
    // client side now can specify a range of the block for checksum
    long requestLength = block.getNumBytes();
    Preconditions.checkArgument(requestLength >= 0);
    long visibleLength = datanode.data.getReplicaVisibleLength(block);
    boolean partialBlk = requestLength < visibleLength;

    updateCurrentThreadName("Reading metadata for block " + block);
    final LengthInputStream metadataIn = datanode.data
        .getMetaDataInputStream(block);
    
    final DataInputStream checksumIn = new DataInputStream(
        new BufferedInputStream(metadataIn, HdfsConstants.IO_FILE_BUFFER_SIZE));
    updateCurrentThreadName("Getting checksum for block " + block);
    try {
      //read metadata file
      final BlockMetadataHeader header = BlockMetadataHeader
          .readHeader(checksumIn);
      final DataChecksum checksum = header.getChecksum();
      final int csize = checksum.getChecksumSize();
      final int bytesPerCRC = checksum.getBytesPerChecksum();
      final long crcPerBlock = csize <= 0 ? 0 : 
        (metadataIn.getLength() - BlockMetadataHeader.getHeaderSize()) / csize;

      final MD5Hash md5 = partialBlk && crcPerBlock > 0 ? 
          calcPartialBlockChecksum(block, requestLength, checksum, checksumIn)
            : MD5Hash.digest(checksumIn);
      if (LOG.isDebugEnabled()) {
        LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
            + ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
      }

      //write reply
      BlockOpResponseProto.newBuilder()
        .setStatus(SUCCESS)
        .setChecksumResponse(OpBlockChecksumResponseProto.newBuilder()             
          .setBytesPerCrc(bytesPerCRC)
          .setCrcPerBlock(crcPerBlock)
          .setMd5(ByteString.copyFrom(md5.getDigest()))
          .setCrcType(PBHelper.convert(checksum.getChecksumType())))
        .build()
        .writeDelimitedTo(out);
      out.flush();
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(checksumIn);
      IOUtils.closeStream(metadataIn);
    }

    //update metrics
    datanode.metrics.addBlockChecksumOp(elapsed());
  }

  @Override
  public void copyBlock(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    updateCurrentThreadName("Copying block " + block);
    // Read in the header
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenSecretManager.AccessMode.COPY);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from " + remoteAddress
            + " for OP_COPY_BLOCK for block " + block + " : "
            + e.getLocalizedMessage());
        sendResponse(ERROR_ACCESS_TOKEN, "Invalid access token");
        return;
      }

    }

    if (!DataXceiverServer.balanceThrottler.acquire()) { // not able to start
      String msg = "Not able to copy block " + block.getBlockId() + " " +
          "to " + peer.getRemoteAddressString() + " because threads " +
          "quota is exceeded.";
      LOG.info(msg);
      sendResponse(ERROR, msg);
      return;
    }

    BlockSender blockSender = null;
    DataOutputStream reply = null;
    boolean isOpSuccess = true;

    try {
      // check if the block exists or not
      blockSender = new BlockSender(block, 0, -1, false, false, true, datanode, 
          null, CachingStrategy.newDropBehind());

      // set up response stream
      OutputStream baseStream = getOutputStream();
      reply = new DataOutputStream(new BufferedOutputStream(
          baseStream, HdfsConstants.SMALL_BUFFER_SIZE));

      // send status first
      writeSuccessWithChecksumInfo(blockSender, reply);
      // send block content to the target
      long read = blockSender.sendBlock(reply, baseStream, 
                                        DataXceiverServer.balanceThrottler);

      datanode.metrics.incrBytesRead((int) read);
      datanode.metrics.incrBlocksRead();
      
      LOG.info("Copied " + block + " to " + peer.getRemoteAddressString());
    } catch (IOException ioe) {
      isOpSuccess = false;
      LOG.info("opCopyBlock " + block + " received exception " + ioe);
      throw ioe;
    } finally {
      DataXceiverServer.balanceThrottler.release();
      if (isOpSuccess) {
        try {
          // send one last byte to indicate that the resource is cleaned.
          reply.writeChar('d');
        } catch (IOException ignored) {
        }
      }
      IOUtils.closeStream(reply);
      IOUtils.closeStream(blockSender);
    }

    //update metrics    
    datanode.metrics.addCopyBlockOp(elapsed());
  }

  @Override
  public void replaceBlock(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo proxySource) throws IOException {
    updateCurrentThreadName("Replacing block " + block + " from " + delHint);

    /* read header */
    block.setNumBytes(DataXceiverServer.estimateBlockSize);
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenSecretManager.AccessMode.REPLACE);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from " + remoteAddress
            + " for OP_REPLACE_BLOCK for block " + block + " : "
            + e.getLocalizedMessage());
        sendResponse(ERROR_ACCESS_TOKEN, "Invalid access token");
        return;
      }
    }

    if (!DataXceiverServer.balanceThrottler.acquire()) { // not able to start
      String msg = "Not able to receive block " + block.getBlockId() +
          " from " + peer.getRemoteAddressString() + " because threads " +
          "quota is exceeded.";
      LOG.warn(msg);
      sendResponse(ERROR, msg);
      return;
    }

    Socket proxySock = null;
    DataOutputStream proxyOut = null;
    Status opStatus = SUCCESS;
    String errMsg = null;
    BlockReceiver blockReceiver = null;
    DataInputStream proxyReply = null;
    
    try {
      // get the output stream to the proxy
      final String dnAddr = proxySource.getXferAddr(connectToDnViaHostname);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to datanode " + dnAddr);
      }
      InetSocketAddress proxyAddr = NetUtils.createSocketAddr(dnAddr);
      proxySock = datanode.newSocket();
      NetUtils.connect(proxySock, proxyAddr, dnConf.socketTimeout);
      proxySock.setSoTimeout(dnConf.socketTimeout);

      OutputStream unbufProxyOut = NetUtils.getOutputStream(proxySock,
          dnConf.socketWriteTimeout);
      InputStream unbufProxyIn = NetUtils.getInputStream(proxySock);
      if (dnConf.encryptDataTransfer && 
          !dnConf.trustedChannelResolver.isTrusted(
              proxySock.getInetAddress())) {
        IOStreamPair encryptedStreams =
            DataTransferEncryptor.getEncryptedStreams(
                unbufProxyOut, unbufProxyIn,
                datanode.blockPoolTokenSecretManager
                    .generateDataEncryptionKey(block.getBlockPoolId()));
        unbufProxyOut = encryptedStreams.out;
        unbufProxyIn = encryptedStreams.in;
      }
      
      proxyOut = new DataOutputStream(new BufferedOutputStream(unbufProxyOut, 
          HdfsConstants.SMALL_BUFFER_SIZE));
      proxyReply = new DataInputStream(new BufferedInputStream(unbufProxyIn,
          HdfsConstants.IO_FILE_BUFFER_SIZE));

      /* send request to the proxy */
      new Sender(proxyOut).copyBlock(block, blockToken);

      // receive the response from the proxy
      
      BlockOpResponseProto copyResponse = BlockOpResponseProto.parseFrom(
          PBHelper.vintPrefixed(proxyReply));

      if (copyResponse.getStatus() != SUCCESS) {
        if (copyResponse.getStatus() == ERROR_ACCESS_TOKEN) {
          throw new IOException("Copy block " + block + " from "
              + proxySock.getRemoteSocketAddress()
              + " failed due to access token error");
        }
        throw new IOException("Copy block " + block + " from "
            + proxySock.getRemoteSocketAddress() + " failed");
      }
      
      // get checksum info about the block we're copying
      ReadOpChecksumInfoProto checksumInfo = copyResponse.getReadOpChecksumInfo();
      DataChecksum remoteChecksum = DataTransferProtoUtil.fromProto(
          checksumInfo.getChecksum());
      // open a block receiver and check if the block does not exist
      blockReceiver = new BlockReceiver(
          block, proxyReply, proxySock.getRemoteSocketAddress().toString(),
          proxySock.getLocalSocketAddress().toString(),
          null, 0, 0, 0, "", null, datanode, remoteChecksum,
          CachingStrategy.newDropBehind());

      // receive a block
      blockReceiver.receiveBlock(null, null, null, null, 
          DataXceiverServer.balanceThrottler, null);
                    
      // notify name node
      datanode.notifyNamenodeReceivedBlock(
          block, delHint, blockReceiver.getStorageUuid());

      LOG.info("Moved " + block + " from " + peer.getRemoteAddressString()
          + ", delHint=" + delHint);
      
    } catch (IOException ioe) {
      opStatus = ERROR;
      errMsg = "opReplaceBlock " + block + " received exception " + ioe; 
      LOG.info(errMsg);
      throw ioe;
    } finally {
      // receive the last byte that indicates the proxy released its thread resource
      if (opStatus == SUCCESS) {
        try {
          proxyReply.readChar();
        } catch (IOException ignored) {
        }
      }
      
      // now release the thread resource
      DataXceiverServer.balanceThrottler.release();
      
      // send response back
      try {
        sendResponse(opStatus, errMsg);
      } catch (IOException ioe) {
        LOG.warn("Error writing reply back to " + peer.getRemoteAddressString());
      }
      IOUtils.closeStream(proxyOut);
      IOUtils.closeStream(blockReceiver);
      IOUtils.closeStream(proxyReply);
    }

    //update metrics
    datanode.metrics.addReplaceBlockOp(elapsed());
  }

  private long elapsed() {
    //return now() - opStartTime;
    return 99;
  }		// TODO TODO sha de tindre en  consideracio

  /**
   * Utility function for sending a response.
   * 
   * @param status status message to write
   * @param message message to send to the client or other DN
   */
  private void sendResponse(Status status,
      String message) throws IOException {
    writeResponse(status, message, getOutputStream());
  }

  private static void writeResponse(Status status, String message, OutputStream out)
  throws IOException {
    BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder()
      .setStatus(status);
    if (message != null) {
      response.setMessage(message);
    }
    response.build().writeDelimitedTo(out);
    out.flush();
  }
  
  private void writeSuccessWithChecksumInfo(BlockSender blockSender,
      DataOutputStream out) throws IOException {

    ReadOpChecksumInfoProto ckInfo = ReadOpChecksumInfoProto.newBuilder()
      .setChecksum(DataTransferProtoUtil.toProto(blockSender.getChecksum()))
      .setChunkOffset(blockSender.getOffset())
      .build();
      
    BlockOpResponseProto response = BlockOpResponseProto.newBuilder()
      .setStatus(SUCCESS)
      .setReadOpChecksumInfo(ckInfo)
      .build();
    response.writeDelimitedTo(out);
    out.flush();
  }
  

  private void checkAccess(OutputStream out, final boolean reply, 
      final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> t,
      final Op op,
      final BlockTokenSecretManager.AccessMode mode) throws IOException {
    if (datanode.isBlockTokenEnabled) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking block access token for block '" + blk.getBlockId()
            + "' with mode '" + mode + "'");
      }
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(t, null, blk, mode);
      } catch(InvalidToken e) {
        try {
          if (reply) {
            BlockOpResponseProto.Builder resp = BlockOpResponseProto.newBuilder()
              .setStatus(ERROR_ACCESS_TOKEN);
            if (mode == BlockTokenSecretManager.AccessMode.WRITE) {
              DatanodeRegistration dnR = 
                datanode.getDNRegistrationForBP(blk.getBlockPoolId());
              // NB: Unconditionally using the xfer addr w/o hostname
              resp.setFirstBadLink(dnR.getXferAddr());
            }
            resp.build().writeDelimitedTo(out);
            out.flush();
          }
          LOG.warn("Block token verification failed: op=" + op
              + ", remoteAddress=" + remoteAddress
              + ", message=" + e.getLocalizedMessage());
          throw e;
        } finally {
          IOUtils.closeStream(out);
        }
      }
    }
  }
}
