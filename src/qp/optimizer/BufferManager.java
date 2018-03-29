/**
 * simple buffer manager that distributes the buffers equally among
 * all the join operators
 **/

package qp.optimizer;


public class BufferManager {

    private static int numBuffer;
    private static int numJoin;
    private static int numSortingExceptJoin;

    private static int buffPerJoin;
    private static int buffForSorting = 0;


    public BufferManager(int numBuffer, int numJoin, boolean isDistinct) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        numSortingExceptJoin += isDistinct ? 1 : 0;

        buffPerJoin = numBuffer / (numJoin + numSortingExceptJoin);
        buffForSorting = numBuffer - buffPerJoin * numJoin;
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

    public static int getBuffersForSorting() {
        return buffForSorting;
    }

}
