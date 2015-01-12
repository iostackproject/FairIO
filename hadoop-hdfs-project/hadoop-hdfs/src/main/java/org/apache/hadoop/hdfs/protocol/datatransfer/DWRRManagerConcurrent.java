package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DWRRDFSClient;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DWRRDataXceiver;
import org.apache.hadoop.hdfs.server.namenode.ByteUtils;
import org.apache.hadoop.hdfs.server.namenode.FairIOController;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;

import static org.apache.hadoop.util.Time.now;

/**
 * Created by DEIM on 28/07/14.
 */
public class DWRRManagerConcurrent {
  public static final Log LOG = LogFactory.getLog(DWRRManagerConcurrent.class);
  public static final String nameWeight = "weight";
  private DataNode datanode;
  private DWRRDFSClient dfs;
  private Object lock = new Object();
  private Configuration conf;
  private int numQueues;
  private int processedNumQueues;
  private long quantumSize;
  private boolean weigthedFairShare;
  private boolean enoughDeficitCounter;
  private int Ninit = 7;
  private PriorityQueue<Float> currentActiveWeights;
  private Daemon threadedDWRR = new Daemon(new ThreadGroup("Fair Thread"),
    new Runnable() {
      public final Log LOG = LogFactory.getLog(Daemon.class);

      @Override
      public void run() {
        while (true) {

          DWRRWeightQueueConcurrent<DWRRRequestObject> queue = null;
          try {
            queue = allRequestsQueue.take();
            LOG.info("CAMAMILLA while process requests is new round for " + queue.getClassId());        // TODO TODO log
            datanode.getMetrics().setQueuedRequests("" + queue.getClassId(), queue.getQueuedRequests());

            float queueDeficitCounter = queue.getDeficitCounter();

            if (!weigthedFairShare) queueDeficitCounter += quantumSize;
            else {
              LOG.info("CAMAMILLA while calcul deficitCounter = " + queueDeficitCounter + " + " + quantumSize + " * " + queue.getWeight() + " / " + maxWeight());        // TODO TODO log
              queueDeficitCounter += quantumSize * queue.getWeight() / maxWeight();
            }
            queue.setDeficitCounter(queueDeficitCounter);

            enoughDeficitCounter = true;

            while (enoughDeficitCounter && queue.size() > 0) {
              DWRRRequestObject request = queue.peek();

              long requestSize = request.getSize();
              LOG.info("CAMAMILLA while queda prou deficitCounter? " + requestSize + " <= " + queueDeficitCounter + " ?");        // TODO TODO log
              if (requestSize <= queueDeficitCounter) {     // Processar petico
                queueDeficitCounter -= requestSize;
                queue.setDeficitCounter(queueDeficitCounter);

                LOG.info("CAMAMILLA " + request.getClassId() + " Thread processar peticio " + request.getOp());        // TODO TODO log
                try {
                  DWRRDataXceiver dXc = request.getdXc();
                  long ini = now();
                  dXc.makeOp(request.getOp());
                  long end = now();
                  long elapsed = (end - ini) * 1000;
                  long throughput = (elapsed == 0 ? -1 : quantumSize / elapsed);
                  LOG.info("CAMAMILLA DWRRManagerConcurrent despres de processar time=" + end + " throughput=" + throughput);      // TODO TODO log
                } catch (Exception e) {
                  LOG.info("CAMAMILLA " + request.getClassId() + " Thread DWRRManagerConcurrent peta " + e);        // TODO TODO log
                }

                queue.updateProcessedRequests();
                queue.updateProcessedBytes(requestSize);

                datanode.getMetrics().incrProcessedRequest("" + queue.getClassId(), requestSize, queue.getWeight());

                queue.poll();
                if (queue.numPendingRequests() == 0) {      // cua servida
                  LOG.info("CAMAMILLA while process numPendingRequests = 0");        // TODO TODO log
                  queue.setDeficitCounter(0F);
                  //allRequestsQueue.poll();
                  processedNumQueues++;
                  datanode.getMetrics().setQueuedRequests("" + queue.getClassId(), queue.getQueuedRequests());
                  LOG.info("CAMAMILLA " + numQueues + " cua " + queue.getClassId() + " buida " + " amb peticions servides= " + queue.getProcessedRequests());        // TODO TODO log
                  String weights = "";
                  for (float weight : currentActiveWeights) {
                    weights += " " + weight;
                  }

                  LOG.info("CAMAMILLA DWRRManagerConcurrent.addOp pesos abans deliminar " + queue.getWeight() + " son {" + weights + "}");      // TODO TODO log

                  currentActiveWeights.remove(queue.getWeight());

                  weights = "";
                  for (float weight : currentActiveWeights) {
                    weights += " " + weight;
                  }

                  LOG.info("CAMAMILLA DWRRManagerConcurrent.addOp pesos despres deliminar " + queue.getWeight() + " son {" + weights + "}");      // TODO TODO log

                  numQueues--;
                  enoughDeficitCounter = false;
                }
              } else {      // Augmentar deficitCounter i encuar i mirar seguent cua
                LOG.info("CAMAMILLA while process Augmentar deficitCounter");        // TODO TODO log
                //allRequestsQueue.poll();

                LOG.info("CAMAMILLA while process encara te peticions, tornar a encuar");        // TODO TODO log
                allRequestsQueue.add(queue);

                enoughDeficitCounter = false;
              }

              LOG.info("CAMAMILLA INI print");          // TODO TODO log
              for (long key : allRequestMap.keySet()) {
                DWRRWeightQueueConcurrent<DWRRRequestObject> queueAux = allRequestMap.get(key);
                LOG.info(now() + "CAMAMILLA " + queueAux.toString());          // TODO TODO log
              }
              String weights = "";
              for (float weight : currentActiveWeights) {
                weights += " " + weight;
              }
              LOG.info("CAMAMILLA DWRRManagerConcurrent.Thread run pesos son {" + weights + "}");      // TODO TODO log
              LOG.info("CAMAMILLA END print");          // TODO TODO log
            }
          } catch (InterruptedException e) {
            LOG.error("CAMAMILLA DWRRManagerConcurrent ", e);
          }
        }
      }
    });
  private Comparator<Float> maxComparator = Collections.reverseOrder();
  private ConcurrentHashMap<Long, DWRRWeightQueueConcurrent<DWRRRequestObject>> allRequestMap;
  private LinkedTransferQueue<DWRRWeightQueueConcurrent<DWRRRequestObject>> allRequestsQueue;

  // TODO TODO fer que totes les classes propies que siguin modificacio duna altra de hadoop siguin per herencia, aixi afavorim la reutilitzacio de codi
  public DWRRManagerConcurrent(Configuration conf, DWRRDFSClient dfs, DataNode datanode) {
    this.conf = conf;
    this.allRequestsQueue = new LinkedTransferQueue<DWRRWeightQueueConcurrent<DWRRRequestObject>>();
    this.allRequestMap = new ConcurrentHashMap<Long, DWRRWeightQueueConcurrent<DWRRRequestObject>>();
    this.quantumSize = conf.getLong(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE_DEFAULT);
    this.weigthedFairShare = conf.getBoolean(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_WEIGTHED_FAIR_SHARE, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_WEIGTHED_FAIR_SHARE_DEFAULT);
    this.numQueues = 0;
    this.processedNumQueues = 0;
    this.currentActiveWeights = new PriorityQueue<Float>(Ninit, maxComparator);
    this.dfs = dfs;
    this.datanode = datanode;

    this.threadedDWRR.start();
  }

  private Float maxWeight() {
    return currentActiveWeights.peek();
  }

  /**
   * Queue
   * boolean 		offer(E e)			Inserts the specified element at the tail of this queue if it is possible to do so immediately without exceeding the queue's capacity, returning true upon success and false if this queue is full.
   * boolean		offer(E e, long timeout, TimeUnit unit)			Inserts the specified element at the tail of this queue, waiting if necessary up to the specified wait time for space to become available.
   * void				put(E e)			Inserts the specified element at the tail of this queue, waiting if necessary for space to become available.
   * E					peek()			Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
   * E					poll()			Retrieves and removes the head of this queue, or returns null if this queue is empty.
   * E					poll(long timeout, TimeUnit unit)			Retrieves and removes the head of this queue, waiting up to the specified wait time if necessary for an element to become available.
   */
  public void addOp(DWRRRequestObject rec, long classId) {
    //synchronized (lock) {
      DWRRWeightQueueConcurrent<DWRRRequestObject> currentRequestQueue;

      if (allRequestMap.get(classId) == null) {
        LOG.info("CAMAMILLA addop " + classId + " no al map");      // TODO TODO log

        Map<String, byte[]> xattr = null;
        float weight;
        try {
          xattr = dfs.getXAttrs(classId, datanode.getDatanodeId().getDatanodeUuid());

          if (xattr == null) {
            LOG.error("CAMAMILLA DWRRDataXceiver.opReadBlock.list no te atribut weight");      // TODO TODO log
            weight = FairIOController.DEFAULT_WEIGHT;
          } else {
            LOG.info("CAMAMILLA DWRRDataXceiver.opReadBlock.list fer el get de user." + DWRRManagerConcurrent.nameWeight);      // TODO TODO log
            weight = ByteUtils.bytesToFloat(xattr.get("user." + DWRRManagerConcurrent.nameWeight));
          }
        } catch (IOException e) {
          LOG.error("CAMAMILLA DWRRDataXceiver.opReadBlock.list ERROR al getXattr " + e.getMessage());      // TODO TODO log
          weight = FairIOController.DEFAULT_WEIGHT;
        }

        currentRequestQueue = new DWRRWeightQueueConcurrent<DWRRRequestObject>(classId, weight, System.currentTimeMillis());
        allRequestMap.put(classId, currentRequestQueue);
      } else {
        LOG.info("CAMAMILLA addop " + classId + " al map");      // TODO TODO log
        currentRequestQueue = allRequestMap.get(classId);
      }

      currentRequestQueue.add(rec);
      if (!allRequestsQueue.contains(currentRequestQueue)) {
        allRequestsQueue.add(currentRequestQueue);

        currentActiveWeights.add(currentRequestQueue.getWeight());

        String weights = "";
        for (float weight : currentActiveWeights) {
          weights += " " + weight;
        }

        LOG.info("CAMAMILLA DWRRManagerConcurrent.addOp pesos despres dafegir " + currentRequestQueue.getWeight() + " son {" + weights + "}");      // TODO TODO log

        numQueues++;
      }

      LOG.info("CAMAMILLA peticio " + classId + " encuada amb pes " + currentRequestQueue.getWeight() + ". Quantes peticions te: " + currentRequestQueue.size());      // TODO TODO log
      //lock.notify();
    //}
  }

}