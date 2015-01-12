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
package org.apache.hadoop.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 ********************************************************/
@InterfaceAudience.Private
public class DWRRDFSClient {
  public static final Log LOG = LogFactory.getLog(DWRRDFSClient.class);

  final ClientProtocol namenode;

  /**
   * Same as this(NameNode.getAddress(conf), conf);
   * @see #DWRRDFSClient(java.net.InetSocketAddress, org.apache.hadoop.conf.Configuration)
   * @deprecated Deprecated at 0.21
   */
  @Deprecated
  public DWRRDFSClient(Configuration conf) throws IOException {
    this(NameNode.getAddress(conf), conf);
  }

  public DWRRDFSClient(InetSocketAddress address, Configuration conf) throws IOException {
    this(NameNode.getUri(address), conf);
  }

  /**
   * Same as this(nameNodeUri, conf, null);
   * @see #DWRRDFSClient(java.net.URI, org.apache.hadoop.conf.Configuration)
   */
  public DWRRDFSClient(URI nameNodeUri, Configuration conf
  ) throws IOException {
    this(nameNodeUri, null, conf);
  }

  /**
   * Create a new DFSClient connected to the given nameNodeUri or rpcNamenode.
   * If HA is enabled and a positive value is set for
   * {@link org.apache.hadoop.hdfs.DFSConfigKeys#DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY} in the
   * configuration, the DFSClient will use {@link org.apache.hadoop.io.retry.LossyRetryInvocationHandler}
   * as its RetryInvocationHandler. Otherwise one of nameNodeUri or rpcNamenode
   * must be null.
   */
  @VisibleForTesting
  public DWRRDFSClient(URI nameNodeUri, ClientProtocol rpcNamenode,
                       Configuration conf)
    throws IOException {

    if (rpcNamenode != null) {
      // This case is used for testing.
      Preconditions.checkArgument(nameNodeUri == null);
      this.namenode = rpcNamenode;
    } else {
      Preconditions.checkArgument(nameNodeUri != null,
          "null URI");
      NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = NameNodeProxies.createProxy(conf, nameNodeUri,
          ClientProtocol.class);
      this.namenode = proxyInfo.getProxy();
    }

  }

  /**
   * Get the namenode associated with this DFSClient object
   * @return the namenode associated with this DFSClient object
   */
  public ClientProtocol getNamenode() {
    return namenode;
  }

  public void setXAttr(String src, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    try {
      namenode.setXAttr(src, XAttrHelper.buildXAttr(name, value), flag);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  public byte[] getXAttr(String src, String name) throws IOException {
    try {
      final List<XAttr> xAttrs = XAttrHelper.buildXAttrAsList(name);
      final List<XAttr> result = namenode.getXAttrs(src, xAttrs);
      return XAttrHelper.getFirstXAttrValue(result);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  public Map<String, byte[]> getXAttrs(String src) throws IOException {
    try {
      return XAttrHelper.buildXAttrMap(namenode.getXAttrs(src, null));
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  public Map<String, byte[]> getXAttrs(String src, List<String> names)
      throws IOException {
    try {
      return XAttrHelper.buildXAttrMap(namenode.getXAttrs(
          src, XAttrHelper.buildXAttrs(names)));
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  public Map<String, byte[]> getXAttrs(long classId, String datanodeId) throws IOException {
    try {
      return XAttrHelper.buildXAttrMap(namenode.getXAttrsCl(classId, datanodeId));
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
        FileNotFoundException.class,
        UnresolvedPathException.class);
    }
  }

  public List<String> listXAttrs(String src)
          throws IOException {
    try {
      final Map<String, byte[]> xattrs =
        XAttrHelper.buildXAttrMap(namenode.listXAttrs(src));
      return Lists.newArrayList(xattrs.keySet());
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  public void removeXAttr(String src, String name) throws IOException {
    try {
      namenode.removeXAttr(src, XAttrHelper.buildXAttr(name));
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     SafeModeException.class,
                                     SnapshotAccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

}
