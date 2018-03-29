/* Sort in memory Operation **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class Sort extends Operator {

    private Operator base;  // base operator
    private Vector attrSet;  //Order by
    private int batchsize;  // number of tuples per outbatch

    private int[] attrIndex;
    private int numBuff;
    int numInputBuff;
    /**
     * The following fields are required during
     * * execution of the Sort operator
     **/
    private static int NUMINSTANCES = 0;

    private int instanceNumber;
    private int numPasses = 0;
    private int numRuns = 0;
    private boolean isExternal = true;  // Indiacate whether we need an external sort
    private boolean eos;  // Indiacate whether end of stream is reached or not
    private List<Tuple> buffer; // main memory
    private Batch[] inbatches; //main memory
    private Scan[] runScanner; // write in tuples, read in batches
    private int[] tupleIndexes; // default value is zero
    private int start = 0;


    /**
     * constructor
     **/

    public Sort(Operator base, Vector as, int numBuff) {
        super(OpType.SORT);
        this.base = base;
        this.attrSet = as;
        this.numBuff = numBuff;
        this.numInputBuff = numBuff - 1;
        this.instanceNumber = NUMINSTANCES++;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    /**
     * Opens the connection to the base operator
     **/

    public boolean open() {
        eos = false;     // Since the stream is just opened

        /* set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        attrIndex = new int[attrSet.size()];
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = schema.indexOf(attr);
            attrIndex[i] = index;
        }

        if (base.open()) {
            buffer = new ArrayList<Tuple>(numBuff * batchsize); // only one buffer, remember to clean
            numPasses = 0;
            numRuns = 0;
            while (true) { // create sorted runs
                int batchIndex;
                /* read the tuples in buffer as much as possible */
                for (batchIndex = 0; batchIndex < numBuff; batchIndex++) {
                    Batch inbatch = base.next();
                    if (inbatch == null || inbatch.size() == 0) {
                        if (numRuns == 0) {
                            /* the buffer is full in the first loop, we don't need external sort */
                            isExternal = false;
                        }
                        break;
                    }
                    for (int tupleindex = 0; tupleindex < inbatch.size(); tupleindex++) {
                        buffer.add(inbatch.elementAt(tupleindex));
                    }
                }
                /* sort the data in buffer */
                buffer.sort((arg0, arg1) -> {
                    for (int anAttrIndex : attrIndex) {
                        int cmpresult = Tuple.compareTuples(arg0, arg1, anAttrIndex);
                        if (cmpresult != 0) {
                            return cmpresult;
                        }
                    }
                    return 0;
                });
                /* if we need an external sort, write the tuples to run files and clear the buffer */
                if (isExternal) {
                    if (numBuff <= 2) {
                        System.out.println("Minimum 3 buffers are required for an external sort operator");
                        System.exit(1);
                    }
                    try {
                        String pathname = "Run-" + instanceNumber + "-" + numPasses + "-" + numRuns + ".tbl";
                        File runFile = new File(pathname);
                        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(runFile));
//                        System.out.println("@@@@"+pathname);
                        for (Tuple tuple : buffer) {
//                            Debug.PPrint(tuple);
                            out.writeObject(tuple);
                        }
                        out.close();
                    } catch (IOException e) {
                        System.out.println("ExternalSort: Error in writing the temporary file");
                    }
                    numRuns++;
                    buffer.clear();
                    /* for this run, the buffer is not full, this means no more tuples */
                    if (batchIndex != numBuff) {
                        break;
                    }
                }
                /* if we need an internal sort, keep the buffer, sign the start pointer and return */
                else {
                    start = 0;
                    return base.close();
                }
            }

            /* if we need an external sort, merge the runs until the last pass */
            inbatches = new Batch[numInputBuff];
            runScanner = new Scan[numInputBuff];
            tupleIndexes = new int[numInputBuff];

            while (numRuns > numInputBuff) {
                int readPass = numPasses;
                numPasses++;
                int numMerge = (numRuns + numInputBuff - 1) / numInputBuff;
                int mergeFrom = 0;
                for (int mergeIndex = 0; mergeIndex < numMerge; mergeIndex++) {
                    int numMergeRuns = Math.min(numInputBuff, numRuns - mergeFrom);
                    openRuns(schema, instanceNumber, readPass, mergeFrom, numMergeRuns);
                    mergeRuns(instanceNumber, numPasses, mergeIndex, numMergeRuns, attrIndex, batchsize, false);
                    mergeFrom += numInputBuff;
                }
                numRuns = numMerge;
            }
            /* leave the runs belong to the last pass open */
            openRuns(schema, instanceNumber, numPasses, 0, numRuns);
            return base.close();
        } else
            return false;
    }

    private void openRuns(Schema schema, int instanceNumber, int numPasses, int mergeFrom, int numMergeRuns) {
        /* read each first batch of sorted files into memory */
        for (int runIndex = 0; runIndex < numMergeRuns; runIndex++) {
            int runNumber = mergeFrom + runIndex;
            String runName = "Run-" + instanceNumber + "-" + numPasses + "-" + runNumber;
            runScanner[runIndex] = new Scan(runName, OpType.SCAN);
            runScanner[runIndex].setSchema(schema);
            if (!runScanner[runIndex].open()) {
                System.out.println("@@@@Oops! open error!");
            }
            Batch inbatch = runScanner[runIndex].next();
            if (inbatch == null || inbatch.size() == 0) {
                tupleIndexes[runIndex] = -1;
            }
            else {
                inbatches[runIndex] = inbatch;
                tupleIndexes[runIndex] = 0;
            }
        }
    }

    private Batch mergeRuns(int instanceNumber, int numPasses, int mergeIndex, int numMergeRuns, int[] attrIndex, int batchsize, boolean isLastMerge) {
        Batch outbatch = new Batch(batchsize);
        int minBatch = -1;
        Tuple minTuple = null;
        boolean isOneLeft = false; // Indiacate whether we have only one run to merge
        ObjectOutputStream out = null;

        /* if it is not the last merge, create new run file*/
        if (!isLastMerge) {
            String pathname = "Run-" + instanceNumber + "-" + numPasses + "-" + mergeIndex + ".tbl";
            File runFile = new File(pathname);
            try {
                out = new ObjectOutputStream(new FileOutputStream(runFile));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        do {
            while (!outbatch.isFull()) {
                if (!isOneLeft) {
                    /* Looking for the next tuple and its batch number */
                    isOneLeft = true;
                    for (int batchIndex = 0; batchIndex < numMergeRuns; batchIndex++) {
                        if (tupleIndexes[batchIndex] != -1) {
                            Tuple curTuple = inbatches[batchIndex].elementAt(tupleIndexes[batchIndex]);
                            if (minTuple == null) {
                                minTuple = curTuple;
                                minBatch = batchIndex;
                            } else {
                                isOneLeft = false; // flag: at least two runs left need to be merged
                                /* compare to find the smaller tuple*/
                                for (int anAttrIndex : attrIndex) {
                                    int cmpresult = Tuple.compareTuples(minTuple, curTuple, anAttrIndex);
                                    if (cmpresult > 0) {
                                        minTuple = curTuple;
                                        minBatch = batchIndex;
                                        break;
                                    } else if (cmpresult < 0) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {// only one run file is left
                    minTuple = inbatches[minBatch].elementAt(tupleIndexes[minBatch]);
                }

                /* deal with output*/
                if (isLastMerge) { // if it is the last merge, add the tuple to output
                    outbatch.add(minTuple); // When should I copy one item?
                } else { // if it is not the last merge, write the tuple to the new run file
                    try {
                        assert out != null;
                        out.writeObject(minTuple);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                minTuple = null;

                /* update tupleIndexes */
                if (tupleIndexes[minBatch] == inbatches[minBatch].size() - 1) {
                    Batch inbatch = runScanner[minBatch].next();
                    if (inbatch == null || inbatch.size() == 0) {
                        tupleIndexes[minBatch] = -1;
                        runScanner[minBatch].close();
                        File f = new File(runScanner[minBatch].getTabName() + ".tbl");
                        f.delete();
                        if (isOneLeft) {
                            /* the last run is exhaustive, it is time to return */
                            if (isLastMerge) {
                                eos = true;
                                return outbatch;
                            } else {
                                try {
                                    out.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                return null;
                            }
                        }
                    }
                    else {
                        tupleIndexes[minBatch] = 0;
                        inbatches[minBatch] = inbatch;
                    }
                } else {
                    tupleIndexes[minBatch]++;
                }
            }
        } while (!isLastMerge);
        return outbatch;
    }

    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/

    public Batch next() {
        //System.out.println("Sort:-----------------in next--------------");

        if (eos) {
            close();
            return null;
        }

        /* An output buffer is initiated**/
        Batch outbatch = new Batch(batchsize);
        int mergeIndex = 0;
        if (isExternal) {
            outbatch = mergeRuns(instanceNumber, numPasses, mergeIndex, numRuns, attrIndex, batchsize, true);
            return outbatch;
        } else {
            /* keep on checking the incoming pages until
              the output buffer is full
             */
            while (!outbatch.isFull()) {
                /* There is no more incoming pages from base operator **/
                if (start == buffer.size()) {
                    eos = true;
                    return outbatch;
                }
                outbatch.add(buffer.get(start));
                start++;
            }
            return outbatch;
        }
    }


    /**
     * closes the output connection
     * * i.e., no more pages to output
     **/

    public boolean close() {
		/*
		 if(base.close())
		 return true;
		 else
		 return false;
		 */
        return true;
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for (int i = 0; i < attrSet.size(); i++)
            newattr.add((Attribute) ((Attribute) attrSet.elementAt(i)).clone());
        Sort newsel = new Sort(newbase, newattr, optype);
        newsel.setSchema(newbase.getSchema());
        return newsel;
    }
}


