/* page nested join algorithm **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;

public class BlockNestedJoin extends Join {


    private int numbatch;  //Number of tuples per out batch
    private int batchsize;  //Number of tuples per out batch
    private int leftbatchsize;  //Number of tuples per in batch
    private int totalsize;

    /**
     * The following fields are useful during execution of
     * * the BlockNestedJoin operation
     **/
    private int leftindex;     // Index of the join attribute in left table
    private int rightindex;    // Index of the join attribute in right table

    private String rfname;    // The file name where the right table is materialize

    private static int filenum = 0;   // To get unique filenum for this operation

    private Batch[] leftbatches;  // Buffer for left input stream
    private Batch rightbatch;  // Buffer for right input stream
    private boolean isbasetable; //Whether the right relation is a base table
    private ObjectInputStream in; // File pointer to the right hand materialized file

    private int lcursi;    // Cursor for left side buffer
    private int lcursj;    // Cursor for left side buffer
    private int rcurs;    // Cursor for right side buffer
    private boolean eosl;  // Whether end of stream (left table) is reached
    private boolean eosr;  // End of stream (right table)

    public BlockNestedJoin(Join jn) {
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
        int lefttuplesize = left.getSchema().getTupleSize();
        leftbatchsize = Batch.getPageSize() / lefttuplesize;
        leftbatches = new Batch[numBuff - 2];

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        /* initialize the cursors of input buffers **/

        lcursi = 0;
        lcursj = 0;
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
                rfname = "BJtemp-" + String.valueOf(filenum);
                try {
                    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                    while ((rightpage = right.next()) != null) {
                        out.writeObject(rightpage);
                    }
                    out.close();
                } catch (IOException io) {
                    System.out.println("NestedJoin:writing the temporay file error");
                    return false;
                }
                if (!right.close())
                    return false;
            }
        }
        return left.open();
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        //System.out.print("BlockNestedJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i, j, k;
        if (eosl) {
            close();
            return null;
        }
        Batch outbatch = new Batch(batchsize);


        while (!outbatch.isFull()) {

            if (lcursi == 0 && lcursj == 0 && eosr) {
                /* new left page is to be fetched**/
//		leftbatches =(Batch) left.next();
                for (k = 0; k < numBuff - 2; ++k) {
                    leftbatches[k] = (Batch) left.next();
                    if (leftbatches[k] == null) {
                        if (k == 0) {
                            eosl = true;
                            return outbatch;
                        }
                        break;
                    }
                }
                numbatch = k;
                totalsize = (numbatch - 1) * leftbatchsize + leftbatches[numbatch - 1].size();

                /* Whenver a new left page came , we have to start the
                  scanning of right table
                 */
                if (isbasetable) {
                    if (!right.open()) {
                        System.out.println("@@@@Oops! open error!");
                        return null;
                    }
                } else {
                    try {
                        in = new ObjectInputStream(new FileInputStream(rfname));
                    } catch (IOException io) {
                        System.err.println("NestedJoin:error in reading the file");
                        System.exit(1);
                    }
                }
                eosr = false;

            }

            while (!eosr) {

                try {
                    if (rcurs == 0 && lcursi == 0 && lcursj == 0) {
                        if (isbasetable) {
                            rightbatch = right.next();
                            if (rightbatch == null) {
                                eosr = true;
                                break;
                            }
                        } else {
                            rightbatch = (Batch) in.readObject();
                        }
                    }

                    for (k = lcursi; k < numbatch; k++) {
                        for (i = lcursj; i < leftbatches[k].size(); i++) {
                            for (j = rcurs; j < rightbatch.size(); j++) {
                                Tuple lefttuple = leftbatches[k].elementAt(i);
                                Tuple righttuple = rightbatch.elementAt(j);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);

                                    //Debug.PPrint(outtuple);
                                    //System.out.println();
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        int index = k * leftbatchsize + i;
                                        int lcurs;
                                        if (index == totalsize - 1 && j == rightbatch.size() - 1) {//case 1
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (index != totalsize - 1 && j == rightbatch.size() - 1) {//case 2
                                            lcurs = index + 1;
                                            rcurs = 0;
                                        } else if (index == totalsize - 1 && j != rightbatch.size() - 1) {//case 3
                                            lcurs = index;
                                            rcurs = j + 1;
                                        } else {
                                            lcurs = index;
                                            rcurs = j + 1;
                                        }
                                        lcursj = lcurs % leftbatchsize;
                                        lcursi = lcurs / leftbatchsize;
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcursj = 0;
                    }
                    lcursi = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin:Error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin:temporary file reading error");
                    System.exit(1);
                }
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











































