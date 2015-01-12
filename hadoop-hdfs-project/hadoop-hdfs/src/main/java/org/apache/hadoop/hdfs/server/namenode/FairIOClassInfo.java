package org.apache.hadoop.hdfs.server.namenode;

import java.math.BigDecimal;

public class FairIOClassInfo implements Comparable<FairIOClassInfo>{
	
	private long classID;
	private BigDecimal weight;
	
	public FairIOClassInfo(long classID) {
		this.classID = classID;
		this.weight = new BigDecimal(100);
	}
	
	public FairIOClassInfo(long classID, float weight) {
		this.classID = classID;
		this.weight = new BigDecimal(weight);
	}
	
	public long getClassID() {
		return this.classID;
	}
	
	public BigDecimal getWeight() {
		return this.weight;
	}
	
	public void setWeight(BigDecimal weight) {
		this.weight = weight;
	}

	public String toString() {
		return String.format("[class: %s, weight: %s]", classID, FairIOController.decimalFormat.format(weight));
	}
	
	@Override
	public int compareTo(FairIOClassInfo o) {
		if (this.classID < o.getClassID()) return -1;
		else if (this.classID == o.getClassID()) return 0;
		else return 1;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this.classID == ((FairIOClassInfo)o).getClassID()) return true;
		else return false;
	}
	
	@Override
	public int hashCode() {
		return new Long(this.classID).hashCode();
	}
	
}
