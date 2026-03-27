package Common.Where;

import Catalog.TableSchema;

import java.util.ArrayList;

public class OrNode implements IWhereOp{

    private IWhereOp left;
    private IWhereOp right;

    public OrNode(IWhereOp n1, IWhereOp n2){
        left = n1;
        right = n2;
    }

    @Override
    public int getPriority() {
        return 1;
    }

    @Override
    public boolean evaluate(ArrayList<Object> tuple, TableSchema tableSchema) {
        if(left != null && right != null){
            return left.evaluate(tuple,tableSchema) || right.evaluate(tuple,tableSchema);
        }
        return false;
    }
}
