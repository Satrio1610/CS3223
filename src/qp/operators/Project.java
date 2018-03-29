/*
  To projec out the required attributes from the result
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.Vector;

public class Project extends Operator {

    private Operator base;
    private Vector attrSet;
    private int batchsize;  // number of tuples per outbatch


    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/

    private boolean eos;  // Indiacate whether end of stream is reached or not
    private Batch inbatch;
    private int start;       // Cursor position in the input buffer

    /**
     * index of the attributes in the base operator
     * * that are to be Projected
     **/

    private int[] attrIndex;


    public Project(Operator base, Vector as, int type) {
        super(type);
        this.base = base;
        this.attrSet = as;

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

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * Projected from the base operator
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
        //System.out.println("Project---Schema: ----------in open-----------");
        //System.out.println("base Schema---------------");
        //Debug.PPrint(baseSchema);
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;

            //  Debug.PPrint(attr);
            //System.out.println("  "+index+"  ");
        }

        return base.open();
    }

    /**
     * Read next tuple from operator
     */

    public Batch next() {
        //System.out.println("Project:-----------------in next-----------------");
        int i = 0;

        if (eos) {
            close();
            return null;
        }

        /* An output buffer is initiated**/
        Batch outbatch = new Batch(batchsize);

        /* all the tuples in the inbuffer goes to the output
         buffer
         */

        /* keep on checking the incoming pages until
          the output buffer is full
         */
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                /* There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }
            //System.out.println("Project:---------------base tuples---------");
            for (i = start; i < inbatch.size(); i++) {
                Tuple basetuple = inbatch.elementAt(i);
                //Debug.PPrint(basetuple);
                //System.out.println();
                Vector present = new Vector();
                for (int j = 0; j < attrSet.size(); j++) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    present.add(data);
                }
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
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
}
