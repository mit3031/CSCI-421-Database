package Common.Where;

import AttributeInfo.AttributeTypeEnum;
import Catalog.TableSchema;
import Common.Logger;

import java.util.ArrayList;

public class RelOpNode implements IWhereOp{
    private IOperandNode left;
    private IOperandNode right;
    private ComparisonOp op;

    public RelOpNode(IOperandNode n1, IOperandNode n2, String relOp) throws BadOperatorException{
        left = n1;
        right = n2;
        op = ComparisonOp.getOp((relOp));
    }

    @Override
    public int getPriority() {
        return 3;
    }

    @Override
    public boolean evaluate(ArrayList<Object> tuple, TableSchema tableSchema) {
        Object val1 = left.getValue(tuple,tableSchema);
        Object val2 = right.getValue(tuple,tableSchema);

        if(left.getType() != right.getType() &&
         !((left.getType() == AttributeTypeEnum.VARCHAR || left.getType() == AttributeTypeEnum.CHAR) &&
                 (right.getType() == AttributeTypeEnum.VARCHAR || right.getType() == AttributeTypeEnum.CHAR))){
            throw new JottTypeMismatchException("Types " + left.getType() + " and " + right.getType() + " Do not match!");
        }

        switch(left.getType()){
            case INTEGER:
                int v1 = (int)val1;
                int v2 = (int)val2;
                return op.compare(v1,v2);
            case DOUBLE:
                double d1 = (double) val1;
                double d2 = (double) val2;
                return op.compare(d1,d2);
            case BOOLEAN:
                //todo
                return false;
            case VARCHAR:
            case CHAR:
                String s1 = (String) val1;
                String s2 = (String) val2;
                return op.compare(s1, s2);
            default:
                System.out.println("How did you even get here......");
                return false;
        }
    }
}
