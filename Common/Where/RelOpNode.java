package Common.Where;

import Catalog.TableSchema;

import java.util.ArrayList;

public class RelOpNode implements IWhereOp{
    private IOperandNode left;
    private IOperandNode right;

    public RelOpNode(IOperandNode n1, IOperandNode n2){
        left = n1;
        right = n2;
    }

    @Override
    public int getPriority() {
        return 3;
    }

    @Override
    public boolean evaluate(ArrayList<Object> tuple, TableSchema tableSchema) {
        Object val1 = left.getValue(tuple,tableSchema);
        Object val2 = left.getValue(tuple,tableSchema);
        //TODO typing makes this annoying perchance
        return false;

    }
}
