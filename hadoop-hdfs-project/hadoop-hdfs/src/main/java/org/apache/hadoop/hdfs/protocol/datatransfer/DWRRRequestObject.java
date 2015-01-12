package org.apache.hadoop.hdfs.protocol.datatransfer;


import org.apache.hadoop.hdfs.server.datanode.DWRRDataXceiver;

/**
 * Created by DEIM on 31/07/14.
 */
public class DWRRRequestObject {
  private static int IDGLOBAL = 0;
  private int id;
  private long size;
	private Op op;
	private long classId;
	private DWRRDataXceiver dXc;
  private Integer requestId;

  public DWRRRequestObject(DWRRDataXceiver dataXceiver, long classId, Op op, long len) {
		this.dXc = dataXceiver;
		this.classId = classId;
		this.op = op;
		this.size = len;
    this.id = IDGLOBAL++;
	}

	public long getClassId() {
		return classId;
	}

	public void setClassId(long classId) {
		this.classId = classId;
	}

	public Op getOp() {
		return op;
	}

	public void setOp(Op op) {
		this.op = op;
	}

	public DWRRDataXceiver getdXc() {
		return dXc;
	}

	public long getSize() {
		return size;
	}

  public String toString() {
    return ""+id;
  }

  public int getRequestId() {
    return id;
  }
}
