/*
  page nested join algorithm
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import javax.swing.plaf.basic.BasicTableHeaderUI;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Vector;

public class SortMergeJoin extends Join {

    private int batchsize;  //Number of tuples per out batch

    /**
     * The following fields are useful during execution of
     * * the SortMergeJoin operation
     **/
    private int leftAttrIndex;     // Index of the join attribute in left table
    private int rightAttrIndex;    // Index of the join attribute in right table

    private Batch leftBatch;
    private Batch rightBatch;
    private Operator leftScaner;
    private Operator rightScaner;

    ObjectOutputStream out;

    private boolean soj;  // Start of join
    private boolean eoj;  // End of join
    private boolean eosl; // End of stream (left table)
    private boolean eosr; // End of stream (right table)
    private boolean nlb;  // Next left batch
    private boolean nrb;  // Next right batch
    private int lcurs;    // current left tuple
    private int rcurs;    // current right tuple
    private Vector<Tuple> leftTuples; // left tuples
    private Vector<Tuple> rightTuples; // right tuples
    private Vector<Tuple> storeTuples; // store tuples
    private int state;

//    private int count;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /**
     * During open finds the index of the join attributes
     * *  Materializes the right hand side into a file
     * *  Opens the connections
     **/


    public boolean open() {

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftAttrIndex = left.getSchema().indexOf(leftattr);
        rightAttrIndex = right.getSchema().indexOf(rightattr);

        Vector leftAs = new Vector();
        leftAs.add(leftattr);
        Vector rightAs = new Vector();
        rightAs.add(rightattr);
        leftScaner = new Sort(left, leftAs, numBuff);
        leftScaner.setSchema(left.getSchema());
        rightScaner = new Sort(right, rightAs, numBuff);
        rightScaner.setSchema(right.getSchema());
        if (!leftScaner.open() || !rightScaner.open()) {
            System.out.println("@@@@Oops! open error!");
            return false;
        }
        Batch inbatch;
        String leftTable = "LeftSortedTable";
        try {
            out = new ObjectOutputStream(new FileOutputStream(leftTable + ".tbl"));
            while ((inbatch = leftScaner.next()) != null) {
                for (int i = 0; i < inbatch.size(); i++) {
                    out.writeObject(inbatch.elementAt(i));
                }
            }
            leftScaner.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        leftScaner = new Scan(leftTable, OpType.SCAN);
        leftScaner.setSchema(left.getSchema());
        if (!leftScaner.open()) {
            System.out.println("@@@@Oops! open error!");
            return false;
        }

        String rightTable = "RightSortedTable";
        try {
            out = new ObjectOutputStream(new FileOutputStream(rightTable + ".tbl"));
            while ((inbatch = rightScaner.next()) != null) {
                for (int i = 0; i < inbatch.size(); i++) {
                    out.writeObject(inbatch.elementAt(i));
                }
            }
            rightScaner.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        rightScaner = new Scan(rightTable, OpType.SCAN);
        rightScaner.setSchema(right.getSchema());
        if (!rightScaner.open()) {
            System.out.println("@@@@Oops! open error!");
            return false;
        }

        /* initialize the cursors of input buffers **/
        state = 0;
        lcurs = 0;
        rcurs = 0;
        soj = true;
        eoj = false;
        nlb = false;
        nrb = false;
        eosl = false;
        eosr = false;
        leftTuples = new Vector<>();
        rightTuples = new Vector<>();
        storeTuples = new Vector<>();

//        count = 0;

        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/

    public Batch next() {

        // end of join
        if (eoj) {
//            System.out.println("==== end of join ====");
            return null;
        }

        // start of join
        if (soj) {
//            System.out.println("==== start of join ====");
            leftBatch = leftScaner.next();
            if (leftBatch == null) {
                eosl = true;
            }
            rightBatch = rightScaner.next();
            if (rightBatch == null) {
                eosr = true;
            }
            soj = false;
        }

        Batch outbatch = new Batch(batchsize);

        while (!storeTuples.isEmpty() && !outbatch.isFull()) {
//            System.out.println("==== restore out tuple ====");
//            count++;
//            System.out.println(count);
            Tuple outTuple = storeTuples.get(0);
//            System.out.println(outTuple.toString());
            outbatch.add(outTuple);
            storeTuples.remove(outTuple);
        }

        while (!outbatch.isFull()) {

            if (eosr && eosl) {
//                System.out.println("==== end of join 1 ====");
                eoj = true;
                return outbatch;
            }

            while (state == 0) {

                // read next batch of left table
                if (nlb) {
                    leftBatch = leftScaner.next();
                    nlb = false;
                    if (leftBatch == null || leftBatch.isEmpty()) {
                        eosl = true;
//                        System.out.println("==== end of stream left ====");
                    }
                }

                // end of left table
                if (eosl) {
                    if (!eosr) {
                        if (rcurs == rightBatch.size() - 1) {
                            rcurs = 0;
                            nrb = true;
                        } else {
                            rcurs++;
                        }
                        state = 1;
                        break;
                    } else {
                        eoj = true;
                        return outbatch;
                    }
                }

                // try to join each tuple in left batch with current right tuple
                for (int i = lcurs; i < leftBatch.size(); i++) {
//                    System.out.println(count);
                    Tuple leftTuple = leftBatch.elementAt(i);
                    if (!rightTuples.isEmpty() && Tuple.compareTuples(leftTuple, rightTuples.get(0), leftAttrIndex, rightAttrIndex) == 0) {
//                        System.out.println("================ left ==============");
//                        System.out.println(leftTuple.data());
//                        System.out.println(rightTuples.get(0).data());
                        for (Tuple tmpTuple : rightTuples) {
                            Tuple outTuple = leftTuple.joinWith(tmpTuple);
//                            System.out.println(outTuple.data());
                            if (!outbatch.isFull()) {
//                                count++;
//                                System.out.println(count);
                                outbatch.add(outTuple);
                            } else {
//                                System.out.println("==== store out tuple ====");
//                                System.out.println(outTuple.toString());
                                storeTuples.add(outTuple);
                            }
                        }
                        if (outbatch.isFull()) {
                            if (i == leftBatch.size() - 1) {
                                lcurs = 0;
                                nlb = true;
                            } else {
                                lcurs = i + 1;
                            }
                            return outbatch;
                        }
                    } else if (!eosr) {
                        // current right tuple exit
                        Tuple rightTuple = rightBatch.elementAt(rcurs);
                        int cmpRst = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);
                        if (cmpRst == 0) {
                            Tuple outTuple = leftTuple.joinWith(rightTuple);
                            outbatch.add(outTuple);
//                            System.out.println(outTuple.data());
//                            count++;
//                            System.out.println(count);
//                            System.out.println("right-------------------  " + count);

                            leftTuples.add(leftTuple);

                            if (outbatch.isFull()) {
                                if (i == leftBatch.size() - 1) {
                                    lcurs = 0;
                                    nlb = true;
                                } else {
                                    lcurs = i + 1;
                                }
                                return outbatch;
                            }
                        } else if (cmpRst > 0) {
                            rightTuples.clear();
                            state = 1;
                            lcurs = i;
                            if (rcurs == rightBatch.size() - 1) {
                                rcurs = 0;
                                nrb = true;
                            } else {
                                rcurs++;
                            }
                            break;
                        } else {
                            leftTuples.clear();
                            rightTuples.clear();
                        }

                        // read next batch
                        if (i == leftBatch.size() - 1) {
                            lcurs = 0;
                            nlb = true;
                        }
                    } else if (eosr) {
//                        System.out.println("==== end of join left ====");
                        eoj = true;
                        return outbatch;
                    }
                }
            }

            while (state == 1) {

                if (nrb) {
                    rightBatch = rightScaner.next();
                    nrb = false;
                    if (rightBatch == null || rightBatch.isEmpty()) {
                        eosr = true;
//                        System.out.println("==== end of stream right ====");
                    }
                }

                if (eosr) {
                    if (!eosl) {
                        if (lcurs == leftBatch.size() - 1) {
                            lcurs = 0;
                            nlb = true;
                        } else {
                            lcurs++;
                        }
                        state = 0;
                        break;
                    } else {
                        eoj = true;
                        return outbatch;
                    }
                }

//                System.out.println(rightBatch.size());
                for (int i = rcurs; i < rightBatch.size(); i++) {
                    Tuple rightTuple = rightBatch.elementAt(i);
                    if (!leftTuples.isEmpty() && Tuple.compareTuples(leftTuples.get(0), rightTuple, leftAttrIndex, rightAttrIndex) == 0) {
//                        System.out.println("================ right ==============");
//                        System.out.println(rightTuple.data());
//                        System.out.println(leftTuples.get(0).data());
                        for (Tuple tmpTuple : leftTuples) {
                            Tuple outTuple = tmpTuple.joinWith(rightTuple);
//                            System.out.println(outTuple.data());
                            if (!outbatch.isFull()) {
//                                count++;
//                                System.out.println(count);
                                outbatch.add(outTuple);
                            } else {
//                                System.out.println("==== store out tuple ====");
//                                System.out.println(outTuple.toString());
                                storeTuples.add(outTuple);
                            }
                        }
//                        System.out.println("================ end right ==============");
                        if (outbatch.isFull()) {
                            if (i == rightBatch.size() - 1) {
                                rcurs = 0;
                                nrb = true;
                            } else {
                                rcurs = i + 1;
                            }
                            return outbatch;
                        }
                    } else if (!eosl) {
                        Tuple leftTuple = leftBatch.elementAt(lcurs);
                        int cmpRst = Tuple.compareTuples(rightTuple, leftTuple, rightAttrIndex, leftAttrIndex);
                        if (cmpRst == 0) {
                            Tuple outTuple = leftTuple.joinWith(rightTuple);
                            outbatch.add(outTuple);
//                            System.out.println(outTuple.data());
//                            count++;
//                            System.out.println("left-------------------  " + count);

                            rightTuples.add(rightTuple);

                            if (outbatch.isFull()) {
                                if (i == rightBatch.size() - 1) {
                                    rcurs = 0;
                                    nrb = true;
                                } else {
                                    rcurs = i + 1;
                                }
                                return outbatch;
                            }
                        } else if (cmpRst > 0) {
                            leftTuples.clear();
                            state = 0;
                            rcurs = i;
                            if (lcurs == leftBatch.size() - 1) {
                                lcurs = 0;
                                nlb = true;
                            } else {
                                lcurs++;
                            }
                            break;
                        } else {
                            leftTuples.clear();
                            rightTuples.clear();
                        }

                        if (i == rightBatch.size() - 1) {
                            rcurs = 0;
                            nrb = true;
                        }
                    } else if (eosl) {
//                        System.out.println("==== end of join right ====");
                        eoj = true;
                        return outbatch;
                    }
                }
            }
        }

        return outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        leftScaner.close();
        rightScaner.close();
        boolean flag1, flag2;
        File f;
        f = new File("LeftSortedTable.tbl");
        flag1 = f.delete();
        f = new File("RightSortedTable.tbl");
        flag2 = f.delete();
        if (!flag1 || !flag2) {
            return false;
        }
        return flag1 && flag2;

    }


}











































