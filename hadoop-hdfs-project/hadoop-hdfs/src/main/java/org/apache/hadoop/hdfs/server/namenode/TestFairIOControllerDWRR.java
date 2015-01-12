package org.apache.hadoop.hdfs.server.namenode;


//import java.math.BigDecimal;
//
//
//public class TestFairIOController {
//
//  /**
//   * @param args
//   */
//  public static void main(String[] args) {
//    FairIOController controller = new FairIOController();
//    DatanodeInfo d1 = new DatanodeInfo(new DatanodeID(), 100);
//    DatanodeInfo d2 = new DatanodeInfo(new DatanodeID(), 200);
//    DatanodeInfo d3 = new DatanodeInfo(new DatanodeID(), 200);
//    ClassInfo c1 = new ClassInfo(1);
//    c1.setWeight(new BigDecimal(100));
//    ClassInfo c2 = new ClassInfo(2);
//    c2.setWeight(new BigDecimal(200));
//    ClassInfo c3 = new ClassInfo(3);
//    c3.setWeight(new BigDecimal(400));
//    // Give an initial weight of 100 (arbitrary)
//    // We should check the weight of the file on the namenode
//    controller.addDatanodeToClass(c1, d3);
//    controller.initializeShares(c1);
//
//    controller.addDatanodeToClass(c2, d1);
//    controller.addDatanodeToClass(c2, d2);
//    controller.initializeShares(c2);
//
//    System.out.println("BEFORE computeSHARES\n"+ controller);
//    controller.computeShares();
//    System.out.println("AFTER computeSHARES\n"+ controller);
//    controller.addDatanodeToClass(c3, d1);
//    controller.addDatanodeToClass(c3, d2);
//    controller.initializeShares(c3);
//    controller.computeShares();
//    System.out.println("AFTER adding c3\n"+ controller);
//
//  }
//}
