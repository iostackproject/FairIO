package org.apache.hadoop.hdfs.server.namenode;

import java.util.Comparator;

public class FairIOMarginalsComparator implements Comparator<FairIODatanodeInfo> {

	@Override
	public int compare(FairIODatanodeInfo arg0, FairIODatanodeInfo arg1) {
		// we put a minus because it's reversed (descending)
		return -arg0.getMarginalValue().compareTo(arg1.getMarginalValue());
	}

}
