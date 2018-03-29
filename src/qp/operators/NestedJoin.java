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

public class NestedJoin extends Join {


    private int batchsize;  //Number of tuples per out batch

    /**
     * The following fields are useful during execution of
     * * the NestedJoin operation
     **/
    private int leftindex;     // Index of the join attribute in left table
    private int rightindex;    // Index of the join attribute in right table

    private String rfname;    // The file name where the right table is materialize

    private static int filenum = 0;   // To get unique filenum for this operation

    private Batch leftbatch;  // Buffer for left input stream
    private Batch rightbatch;  // Buffer for right input stream
    private boolean isbasetable; //Whether the right relation is a base table
    //    ObjectInputStream in; // File pointer to the right hand materialized file
    private Operator rightscaner;

    private int lcurs;    // Cursor for left side buffer
    private int rcurs;    // Cursor for right side buffer
    private boolean eosl;  // Whether end of stream (left table) is reached
    private boolean eosr;  // End of stream (right table)

    public NestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        isbasetable = (right.getOpType() == OpType.SCAN);
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
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        /* initialize the cursors of input buffers **/

        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /* because right stream is to be repetitively scanned
          if it reached end, we have to start new scan
         */
        eosr = true;

        /* If the right operator is not a base table then
          Materialize the intermediate result from right
          into a file
         */
        if (!isbasetable) {
            if (!right.open()) {
                return false;
            } else {
                filenum++;
                rfname = "NJtemp-" + String.valueOf(filenum);
                try {
                    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname + ".tbl"));
                    while ((rightpage = right.next()) != null) {
                        for (int i = 0; i < rightpage.size(); i++) {
                            out.writeObject(rightpage.elementAt(i));
                        }
//                        out.writeObject(rightpage);
                    }
                    out.close();
                } catch (IOException io) {
                    System.out.println("NestedJoin:writing the temporay file error");
                    return false;
                }
                if (!right.close())
                    return false;
                rightscaner = new Scan(rfname, OpType.SCAN);
                rightscaner.setSchema(right.getSchema());
            }
        } else {
            rightscaner = right;
        }
        return left.open();
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        //System.out.print("NestedJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i, j;
        if (eosl) {
            close();
            return null;
        }
        Batch outbatch = new Batch(batchsize);


        while (!outbatch.isFull()) {

            if (lcurs == 0 && eosr == true) {
                /* new left page is to be fetched**/
                leftbatch = (Batch) left.next();
                if (leftbatch == null) {
                    eosl = true;
                    return outbatch;
                }
                /* Whenver a new left page came , we have to start the
                  scanning of right table
                 */
                if (!rightscaner.open()) {
                    System.out.println("@@@@Oops! open error!");
                    return null;
                }
                eosr = false;
            }

            while (!eosr) {

                if (rcurs == 0 && lcurs == 0) {
                    rightbatch = rightscaner.next();
                    if (rightbatch == null) {
                        eosr = true;
                        rightscaner.close();
                        break;
                    }
                }

                for (i = lcurs; i < leftbatch.size(); i++) {
                    for (j = rcurs; j < rightbatch.size(); j++) {
                        Tuple lefttuple = leftbatch.elementAt(i);
                        Tuple righttuple = rightbatch.elementAt(j);
                        if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                            Tuple outtuple = lefttuple.joinWith(righttuple);

                            //Debug.PPrint(outtuple);
                            //System.out.println();
                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                if (i == leftbatch.size() - 1 && j == rightbatch.size() - 1) {//case 1
                                    lcurs = 0;
                                    rcurs = 0;
                                } else if (i != leftbatch.size() - 1 && j == rightbatch.size() - 1) {//case 2
                                    lcurs = i + 1;
                                    rcurs = 0;
                                } else if (i == leftbatch.size() - 1 && j != rightbatch.size() - 1) {//case 3
                                    lcurs = i;
                                    rcurs = j + 1;
                                } else {
                                    lcurs = i;
                                    rcurs = j + 1;
                                }
                                return outbatch;
                            }
                        }
                    }
                    rcurs = 0;
                }
                lcurs = 0;
            }
        }
        return outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        if (!isbasetable) {
            File f = new File(rfname);
            f.delete();
        }
        return true;

    }


}











































