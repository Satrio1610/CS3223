/* Sort in memory Operation **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class SortInternal extends Operator {

    private Operator base;  // base operator
    private Vector attrSet;
    private int batchsize;  // number of tuples per outbatch

    private int[] attrIndex;

    /**
     * The following fields are required during
     * * execution of the SortInternal operator
     **/

    private boolean eos;  // Indiacate whether end of stream is reached or not
    private Batch inbatch;   // This is the current input buffer
    private List<Tuple> buffer;
    private int start;       // Cursor position in the input buffer


    /**
     * constructor
     **/

    public SortInternal(Operator base, Vector as, int type) {
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

    /**
     * Opens the connection to the base operator
     **/

    public boolean open() {
        eos = false;     // Since the stream is just opened
        start = 0;   // set the cursor to starting position in input buffer

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
            buffer = new ArrayList<Tuple>();
            do {
                inbatch = base.next();
                if (inbatch == null)
                    break;
                for (int i = 0; i < inbatch.size(); i++) {
                    buffer.add(inbatch.elementAt(i));
                }
            } while (true);
            buffer.sort((arg0, arg1) -> {
                for (int anAttrIndex : attrIndex) {
                    int cmpresult = Tuple.compareTuples(arg0, arg1, anAttrIndex);
                    if (cmpresult != 0) {
                        return cmpresult;
                    }
                }
                return 0;
            });
            return true;
        } else
            return false;
    }


    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/

    public Batch next() {
        //System.out.println("SortInternal:-----------------in next--------------");

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
        SortInternal newsel = new SortInternal(newbase, newattr, optype);
        newsel.setSchema(newbase.getSchema());
        return newsel;
    }
}



