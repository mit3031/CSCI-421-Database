package Common.Where;

import Catalog.TableSchema;

import java.util.ArrayList;

public interface IWhereOp {

    /**
     * Gets the "priority level" of the operand, for the Shunting-Yard Algorithm
     * 1 is highest priority,
     * @return the priority level
     */
    public int getPriority();

    /**
     * Evaluates a tuple
     * @return whether or not the tuple satistfies this WhereOp
     */
    public boolean evaluate(ArrayList<Object> tuple, TableSchema tableSchema);


}
