package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

/**
 * Created by DEIM on 26/09/14.
 */
public class DWRRWeightQueueConcurrent<E extends DWRRRequestObject> implements TransferQueue<E> {

  private long insertionTime;
  private long processedBytes;
  private LinkedTransferQueue<E> requests;
  private float deficitCounter;
  private float weight;
  private int processedRequests;
  private int totalRequests;
  private long classId;
  private boolean newRound;

  public DWRRWeightQueueConcurrent(long classId, float weight, long time) {
    this.weight = weight;
    this.classId = classId;
    this.deficitCounter = 0;
    this.requests = new LinkedTransferQueue<E>();
    this.processedRequests = 0;
    this.totalRequests = 0;
    this.processedBytes = 0;
    this.newRound = true;
    this.insertionTime = time;
  }

  public long getProcessedBytes() {
    return processedBytes;
  }

  public float getWeight() {
    return weight;
  }

  public long getClassId() {
    return classId;
  }

  public int getProcessedRequests() {
    return processedRequests;
  }

  public void updateProcessedRequests() {
    this.processedRequests++;
  }

  public void updateProcessedBytes(long num) {
    this.processedBytes+=num;
  }

  public float getDeficitCounter() {
    return deficitCounter;
  }

  public void setDeficitCounter(float defC) {
    this.deficitCounter = defC;
  }

  public int numPendingRequests() {
    return this.size();
  }

  @Override
  public String toString() {
    return "Insercio " + insertionTime + "\n\tclassId " + classId + "\t\tpes " + weight + "\t\tdeficitCounter " + deficitCounter + "\t\t" + processedRequests + ":" + totalRequests + " " + "\n\tEncuades " +requests;
  }

  @Override
  public int size() {
    return this.requests.size();
  }

  @Override
  public boolean isEmpty() {
    return this.requests.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.requests.contains(o);
  }

  @Override
  public int drainTo(Collection<? super E> objects) {
    return requests.drainTo(objects);
  }

  @Override
  public int drainTo(Collection<? super E> objects, int i) {
    return requests.drainTo(objects, i);
  }

  @Override
  public Iterator<E> iterator() {
    return this.requests.iterator();
  }

  @Override
  public Object[] toArray() {
    return this.requests.toArray();
  }

  @Override
  public <T> T[] toArray(T[] ts) {
    return this.requests.toArray(ts);
  }

  @Override
  public boolean add(E e) {
    this.totalRequests++;
    return this.requests.add(e);
  }

  @Override
  public boolean remove(Object o) {
    return this.requests.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> objects) {
    return this.requests.containsAll(objects);
  }

  @Override
  public boolean addAll(Collection<? extends E> es) {
    return this.requests.addAll(es);
  }

  @Override
  public boolean removeAll(Collection<?> objects) {
    return this.requests.removeAll(objects);
  }

  @Override
  public boolean retainAll(Collection<?> objects) {
    return this.requests.retainAll(objects);
  }

  @Override
  public void clear() {
    this.requests.clear();
  }

  @Override
  public boolean offer(E e) {
    return this.requests.offer(e);
  }

  @Override
  public void put(E e) throws InterruptedException {
    requests.put(e);
  }

  @Override
  public boolean offer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
    return requests.offer(e, l, timeUnit);
  }

  @Override
  public E take() throws InterruptedException {
    return requests.take();
  }

  @Override
  public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
    return requests.poll(l, timeUnit);
  }

  @Override
  public int remainingCapacity() {
    return requests.remainingCapacity();
  }

  @Override
  public E remove() {
    return this.requests.remove();
  }

  @Override
  public E poll() {
    return this.requests.poll();
  }

  @Override
  public E element() {
    return this.requests.element();
  }

  @Override
  public E peek() {
    return this.requests.peek();
  }

  public boolean isNewRound() {
    return newRound;
  }

  public void setNewRound(boolean nR) {
    newRound = nR;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DWRRWeightQueueConcurrent)) return false;

    DWRRWeightQueueConcurrent that = (DWRRWeightQueueConcurrent) o;

    if (classId != that.classId) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (classId ^ (classId >>> 32));
    return result;
  }

  public int getQueuedRequests() {
    return requests.size();
  }

  @Override
  public boolean tryTransfer(E e) {
    return requests.tryTransfer(e);
  }

  @Override
  public void transfer(E e) throws InterruptedException {
    requests.transfer(e);
  }

  @Override
  public boolean tryTransfer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
    return requests.tryTransfer(e, l, timeUnit);
  }

  @Override
  public boolean hasWaitingConsumer() {
    return requests.hasWaitingConsumer();
  }

  @Override
  public int getWaitingConsumerCount() {
    return requests.getWaitingConsumerCount();
  }
}
