/*
  page nested join algorithm
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

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
    private Batch subLeftBatch;
    private Batch rightBatch;
    private Operator leftScaner;
    private Operator subLeftScaner;
    private Operator rightScaner;

    ObjectOutputStream out;

    private int leftStart = -1;
    private int subLStart = -1;
    private int rightStart = -1;
    private Tuple leftLastTuple;
    private int leftFirstMatchIdx = -1;
    private int rightFirstMatchIdx = -1;
    private int leftMidMatchIdx = -1;
    private int rightMidMatchIdx = -1;
    private boolean eos;  // Whether end of join
    private boolean eols;  // Whether end of join
    private boolean eoslb;  // Whether end of left batch
    private boolean eolb;  // Whether end of left batch
    private boolean eorb;  // Whether end of right batch
    private int mergeState = 0;

    static int counter = 0;
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

        /* select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftAttrIndex = left.getSchema().indexOf(leftattr);
        rightAttrIndex = right.getSchema().indexOf(rightattr);
        Batch rightpage;

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
        leftStart = 0;
        eolb = true;
        rightStart = 0;
        eorb = true;

        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        //System.out.print("SortMergeJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i, j;

        if (eos) {
            return null;
        }
        Batch outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            /* deal with read and end flag */
            if (leftStart == 0 && eolb) {
                leftBatch = leftScaner.next();
                if (leftBatch == null || leftBatch.isEmpty()) {
                    if (mergeState == 3) {
                        mergeState = 5;
                        try {
                            out.close();
                            counter--;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        rightFirstMatchIdx = 0;
                        rightStart = 0;
                        eorb = true;
                        eols = true;
                    }
                    else {
                        eos = true;
                    }
//                    return outbatch;
                }
                eolb = false;
            }
            if (rightStart == 0 && eorb) {
                rightBatch = rightScaner.next();
                if (rightBatch == null || rightBatch.isEmpty()) {
                    eos = true;
//                    return outbatch;
                }
                eorb = false;
            }
            /* deal with compare and state transition */
            Tuple leftTuple = null;
            Tuple rightTuple = null;
            int cmpRst = 0;
            if (!eos) {
                leftTuple = leftBatch.elementAt(leftStart);
                rightTuple = rightBatch.elementAt(rightStart);
                cmpRst = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIndex, rightAttrIndex);
                int leftattr = (int) leftTuple.dataAt(leftAttrIndex);
                int rightattr = (int) rightTuple.dataAt(rightAttrIndex);
//                if (leftattr == 199 || rightattr == 199) {
//                    if (schema.getTupleSize() == 400 && left.getSchema().getTupleSize() == 200)
//                        System.out.println("hh");
//                }
            }
            switch (mergeState) {
                case 0:
                    if (eos) {
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        if (leftStart != leftBatch.size() - 1) {
                            leftStart++;
                        }
                        else {
                            leftStart = 0;
                            eolb = true;
                        }
                    }
                    else if (cmpRst > 0) {
                        if (rightStart != rightBatch.size() - 1) {
                            rightStart++;
                        }
                        else {
                            rightStart = 0;
                            eorb = true;
                        }
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftTuple.joinWith(rightTuple);
                        outbatch.add(outtuple);
                        leftFirstMatchIdx = leftStart;
                        rightFirstMatchIdx = rightStart;
                        leftLastTuple = leftTuple;
                        if (leftStart != leftBatch.size() - 1) {
                            mergeState = 1;
                            leftStart++;
                        }
                        else if (rightStart != rightBatch.size() - 1) {
                            mergeState = 2;
                            rightStart++;
                            leftMidMatchIdx = leftFirstMatchIdx;
                        }
                        else {
                            mergeState = 3;
                            Object joinAttr = leftTuple.dataAt(leftAttrIndex);
                            try {
                                out = new ObjectOutputStream(new FileOutputStream("SMJtemp.tbl"));
                                counter++;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            flushCurrentLeftBatch(leftBatch, leftFirstMatchIdx, leftBatch.size(), joinAttr);
                            leftStart = 0;
                            eolb = true;
                            rightMidMatchIdx = rightFirstMatchIdx;
                        }
                    }
                    break;
                case 1:
                    if (eos) {
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        System.out.println("@@@@Something wrong happens during merge join!");
                        System.exit(1);
                    }
                    else if (cmpRst > 0) {
                        mergeState = 7;
                        leftMidMatchIdx = leftFirstMatchIdx;
                        if (rightStart != rightBatch.size() - 1) {
                            rightStart++;
                        } else {
                            rightStart = 0;
                            eorb = true;
                        }
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftTuple.joinWith(rightTuple);
                        outbatch.add(outtuple);
                        if (leftStart != leftBatch.size() - 1) {
                            leftStart++;
                        }
                        else if (rightStart != rightBatch.size() - 1) {
                            mergeState = 2;
                            rightStart++;
                            leftMidMatchIdx = leftFirstMatchIdx;
                        }
                        else {
                            mergeState = 3;
                            Object joinAttr = leftTuple.dataAt(leftAttrIndex);
                            try {
                                out = new ObjectOutputStream(new FileOutputStream("SMJtemp.tbl"));
                                counter++;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            flushCurrentLeftBatch(leftBatch, leftFirstMatchIdx, leftBatch.size(), joinAttr);
                            leftStart = 0;
                            eolb = true;
                            rightMidMatchIdx = rightFirstMatchIdx;
                        }
                    }
                    break;
                case 2:
                    if (eos) {
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        mergeState = 0;
                        if (leftStart != leftBatch.size() - 1) {
                            leftStart++;
                        }
                        else {
                            leftStart = 0;
                            eolb = true;
                        }
                    }
                    else if (cmpRst > 0) {
                        System.out.println("@@@@Something wrong happens during merge join!");
                        System.exit(1);
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftBatch.elementAt(leftMidMatchIdx).joinWith(rightTuple);
                        outbatch.add(outtuple);
                        leftMidMatchIdx++;
                        if (leftMidMatchIdx == leftBatch.size()) {
                            if (rightStart != rightBatch.size() - 1) {
                                rightStart++;
                                leftMidMatchIdx = leftFirstMatchIdx;
                            }
                            else {
                                mergeState = 3;
                                Object joinAttr = leftTuple.dataAt(leftAttrIndex);
                                try {
                                    out = new ObjectOutputStream(new FileOutputStream("SMJtemp.tbl"));
                                    counter++;
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                flushCurrentLeftBatch(leftBatch, leftFirstMatchIdx, leftBatch.size(), joinAttr);
                                leftStart = 0;
                                eolb = true;
                                leftMidMatchIdx = -1;
                                rightMidMatchIdx = rightFirstMatchIdx;
                            }
                        }
                    }
                    break;
                case 3:
                    if (eos) {
                        try {
                            out.close();
                            counter--;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        System.out.println("@@@@Something wrong happens during merge join!");
                        System.exit(1);
                    }
                    else if (cmpRst > 0) {
                        mergeState = 5;
                        Object joinAttr = leftTuple.dataAt(leftAttrIndex);
                        flushCurrentLeftBatch(leftBatch, 0, leftStart, joinAttr);
                        try {
                            out.close();
                            counter--;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        rightFirstMatchIdx = 0;
                        rightStart = 0;
                        eorb = true;
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftTuple.joinWith(rightBatch.elementAt(rightMidMatchIdx));
                        outbatch.add(outtuple);
                        rightMidMatchIdx++;
                        if (rightMidMatchIdx == rightBatch.size()) {
                            rightMidMatchIdx = rightFirstMatchIdx;
                            if (leftStart != leftBatch.size() - 1) {
                                leftStart++;
                            } else {
                                mergeState = 3;
                                Object joinAttr = leftTuple.dataAt(leftAttrIndex);
                                flushCurrentLeftBatch(leftBatch, 0, leftBatch.size(), joinAttr);
                                leftStart = 0;
                                eolb = true;
                            }
                        }
                    }
                    break;
                case 4:
                    /* dependencies: subLStart, rightFirstMatchIdx, subLeftScaner, */
                    if (eos) {
                        subLeftScaner.close();
                        return outbatch;
                    }
                    if (subLStart == 0 && eoslb) {
                        subLeftBatch = subLeftScaner.next();
                        if (subLeftBatch == null || subLeftBatch.isEmpty()) {
                            mergeState = 5;
                            subLeftScaner.close();
                            rightFirstMatchIdx = 0;
                            rightStart = 0;
                            eorb = true;
                            break;
                        }
                        else {
                            eoslb = false;
                            rightMidMatchIdx = rightFirstMatchIdx;
                        }
                    }
                    for (i = subLStart; i < subLeftBatch.size(); i++) {
                        for (j = rightMidMatchIdx; j < rightBatch.size(); j++) {
                            Tuple lefttuple = subLeftBatch.elementAt(i);
                            Tuple righttuple = rightBatch.elementAt(j);
                            Tuple outtuple = lefttuple.joinWith(righttuple);

                            //Debug.PPrint(outtuple);
                            //System.out.println();
                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                if (i == subLeftBatch.size() - 1 && j == rightBatch.size() - 1) {//case 1
                                    subLStart = 0;
                                    eoslb = true;
                                    rightMidMatchIdx = rightFirstMatchIdx;
                                } else if (i != subLeftBatch.size() - 1 && j == rightBatch.size() - 1) {//case 2
                                    subLStart = i + 1;
                                    rightMidMatchIdx = rightFirstMatchIdx;
                                } else {
                                    subLStart = i;
                                    rightMidMatchIdx = j + 1;
                                }
                                return outbatch;
                            }
                        }
                        rightMidMatchIdx = rightFirstMatchIdx;
                    }
                    subLStart = 0;
                    eoslb = true;
                    break;
                case 5:
                    if (eos) {
                        return outbatch;
                    }
                    cmpRst = Tuple.compareTuples(leftLastTuple, rightTuple, leftAttrIndex, rightAttrIndex);
                    if (cmpRst < 0) {
                        if (eols) {
                            eos = true;
                        }
                        mergeState = 6;
                        subLeftScaner = new Scan("SMJtemp", OpType.SCAN);
                        subLeftScaner.setSchema(left.getSchema());
                        if (!subLeftScaner.open()) {
                            System.out.println("@@@@Oops! open error!");
                            System.exit(2);
                        }
                        subLStart = 0;
                        eoslb = true;
                        rightMidMatchIdx = 0;
                        assert rightFirstMatchIdx == 0;
                    }
                    else if (cmpRst > 0) {
                        System.out.println("@@@@Something wrong happens during merge join!");
                        System.exit(1);
                    }
                    else {// cmpRst == 0
                        if (rightStart != rightBatch.size() - 1) {
                            rightStart++;
                        }
                        else {
                            mergeState = 4;
                            subLeftScaner = new Scan("SMJtemp", OpType.SCAN);
                            subLeftScaner.setSchema(left.getSchema());
                            if (!subLeftScaner.open()) {
                                System.out.println("@@@@Oops! open error!");
                                System.exit(2);
                            }
                            subLStart = 0;
                            eoslb = true;
                            rightMidMatchIdx = rightFirstMatchIdx;
                            assert rightFirstMatchIdx==0;
                        }
                    }
                    break;
                case 6:
                    /* dependencies: subLStart, rightFirstMatchIdx, subLeftScaner, rightStart, */
                    if (eos) {
                        subLeftScaner.close();
                        return outbatch;
                    }
                    if (subLStart == 0 && eoslb) {
                        subLeftBatch = subLeftScaner.next();
                        if (subLeftBatch == null || subLeftBatch.isEmpty()) {
                            mergeState = 0;
                            subLeftScaner.close();
                            break;
                        }
                        else {
                            eoslb = false;
                            rightMidMatchIdx = 0;
                        }
                    }
                    for (i = subLStart; i < subLeftBatch.size(); i++) {
                        for (j = rightMidMatchIdx; j < rightStart; j++) {
                            Tuple lefttuple = subLeftBatch.elementAt(i);
                            Tuple righttuple = rightBatch.elementAt(j);
                            Tuple outtuple = lefttuple.joinWith(righttuple);

                            //Debug.PPrint(outtuple);
                            //System.out.println();
                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                if (i == subLeftBatch.size() - 1 && j == rightStart) {//case 1
                                    subLStart = 0;
                                    eoslb = true;
                                    rightMidMatchIdx = 0;
                                } else if (i != subLeftBatch.size() - 1 && j == rightStart) {//case 2
                                    subLStart = i + 1;
                                    rightMidMatchIdx = 0;
                                } else {
                                    subLStart = i;
                                    rightMidMatchIdx = j + 1;
                                }
                                return outbatch;
                            }
                        }
                        rightMidMatchIdx = 0;
                    }
                    subLStart = 0;
                    eoslb = true;
                    break;
                case 7:
                    if (eos) {
                        return outbatch;
                    }
                    cmpRst = Tuple.compareTuples(leftLastTuple, rightTuple, leftAttrIndex, rightAttrIndex);
                    if (cmpRst < 0) {
                        mergeState = 0;
                    }
                    else if (cmpRst > 0) {
                        System.out.println("@@@@Something wrong happens during merge join!");
                        System.exit(1);
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftBatch.elementAt(leftMidMatchIdx).joinWith(rightTuple);
                        outbatch.add(outtuple);
                        leftMidMatchIdx++;
                        if (leftMidMatchIdx == leftBatch.size() || leftMidMatchIdx == leftStart) {
                            leftMidMatchIdx = leftFirstMatchIdx;
                            if (rightStart != rightBatch.size() - 1) {
                                rightStart++;
                            }
                            else {
                                rightStart = 0;
                                eorb = true;
                            }
                        }
                    }
                    break;
            }
        }
        return outbatch;
    }

    private void flushCurrentLeftBatch(Batch leftBatch, int leftTupleFrom, int leftTupleTo, Object joinAttr) {
        try {
            for (int i = leftTupleFrom; i < leftTupleTo; i++) {
                out.writeObject(leftBatch.elementAt(i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Close the operator
     */
    public boolean close() {
        leftScaner.close();
        rightScaner.close();
        System.out.println("out counter:" + counter);
        boolean flag1, flag2, flag3;
        File f = new File("SMJtemp.tbl");
//        subLeftScaner.close();
        flag1 = f.delete(); // I dont't know why, but I can not delete this file successfully.
        System.out.println(flag1);
        f = new File("LeftSortedTable.tbl");
        flag2 = f.delete();
        f = new File("RightSortedTable.tbl");
        flag3 = f.delete();
        if (!flag2 || !flag3) {
            return false;
        }
        return flag2 && flag3;

    }


}











































