/*
  To projec out the required attributes from the result
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.Vector;

public class ProjectDuplicateElimination extends Operator {

    private Operator base;
    private Vector attrSet;
    private int batchsize;  // number of tuples per outbatch
    private Operator sortedfile;
    private int numBuff;

    /**
     * The following fields are requied during execution
     * * of the ProjectDuplicateElimination Operator
     **/

    private boolean eos;  // Indiacate whether end of stream is reached or not
    private Batch inbatch;
    private int start;       // Cursor position in the input buffer

    /**
     * index of the attributes in the base operator
     * * that are to be ProjectDuplicateEliminationed
     **/

    private int[] attrIndex;


    public ProjectDuplicateElimination(Operator base, Vector as, int type, int numBuff) {
        super(type);
        this.base = base;
        this.attrSet = as;
        this.numBuff = numBuff;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public Vector getProjAttr() {
        return attrSet;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * ProjectDuplicateEliminationed from the base operator
     **/

    public boolean open() {
        eos = false;
        start = 0;
        /* set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;


        /* The following loop find outs the index of the columns that
          are required from the base operator
         */

        Schema baseSchema = base.getSchema();
//        int basetuplesize = base.getSchema().getTupleSize();
        attrIndex = new int[attrSet.size()];
        //System.out.println("ProjectDuplicateElimination---Schema: ----------in open-----------");
        //System.out.println("base Schema---------------");
        //Debug.PPrint(baseSchema);
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;

            //  Debug.PPrint(attr);
            //System.out.println("  "+index+"  ");
        }
//        sortedfile = new SortInternal(base, attrSet, OpType.SORT);
        sortedfile = new Sort(base, attrSet, numBuff);
        sortedfile.setSchema(baseSchema);
//        sortedfile = base;
        return sortedfile.open();
    }

    /**
     * Read next tuple from operator
     */

    public Batch next() {
        //System.out.println("ProjectDuplicateElimination:-----------------in next-----------------");
        int i = 0;

        if (eos) {
            close();
            return null;
        }

        /* An output buffer is initiated**/
        Batch outbatch = new Batch(batchsize);


        /* keep on checking the incoming pages until
          the output buffer is full
         */
        Vector lastVector = null;
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = sortedfile.next();
                /* There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }
            //System.out.println("ProjectDuplicateElimination:---------------base tuples---------");
            for (i = start; i < inbatch.size(); i++) {
                Tuple basetuple = inbatch.elementAt(i);
                //Debug.PPrint(basetuple);
                //System.out.println();
                Vector present = new Vector();
                for (int j = 0; j < attrSet.size(); j++) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    present.add(data);
                }
                if (present.equals(lastVector)) {
                    continue;
                }

                lastVector = present;
                Tuple outtuple = new Tuple(present);
                outbatch.add(outtuple);

                if (outbatch.isFull()) {
                    if (i == inbatch.size()) {
                        start = 0;
                    } else {
                        start = i + 1;
                    }
                    return outbatch;
                }
            }
            start = 0;
        }
        return outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        sortedfile.close();
        return true;
		/*
	if(base.close())
	    return true;
	else
	    return false;
	    **/
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for (int i = 0; i < attrSet.size(); i++)
            newattr.add((Attribute) ((Attribute) attrSet.elementAt(i)).clone());
        ProjectDuplicateElimination newproj = new ProjectDuplicateElimination(newbase, newattr, optype, numBuff);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
}
