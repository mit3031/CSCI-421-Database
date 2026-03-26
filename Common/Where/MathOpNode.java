package Common.Where;

import AttributeInfo.AttributeTypeEnum;
import Catalog.TableSchema;

import java.util.List;

public class MathOpNode implements IOperandNode{
    private IOperandNode left;
    private IOperandNode right;
    private MathOp operation;
    public MathOpNode(IOperandNode left, IOperandNode right, String op) throws BadOperatorException{
        this.left = left;
        this.right = right;
        operation = MathOp.getOp(op);
    }


    @Override
    public Object getValue(List<Object> tuple, TableSchema tableSchema) {
        Object valLeft = left.getValue(tuple, tableSchema);
        Object valRight = right.getValue(tuple,tableSchema);
        //check for null value and return null if so
        if (valLeft == null || valRight == null){
            return null;
        }
        //each case for typing of math op
        if(getType() == AttributeTypeEnum.DOUBLE){
            double l = (double) valLeft;
            double r = (double) valRight;
            return operation.calculate(l, r);
        }
        else if(getType() == AttributeTypeEnum.INTEGER){
            int l = (int) valLeft;
            int r = (int) valRight;
            return operation.calculate(l,r);
        }
        else{
            throw new JottTypeMismatchException("Type " + getType() + " Not Supported for Math Operation!");
        }
    }

    @Override
    public AttributeTypeEnum getType() {
        if(left.getType().equals(right.getType())){
            if(left.getType() != AttributeTypeEnum.DOUBLE && left.getType() != AttributeTypeEnum.INTEGER){
                throw new JottTypeMismatchException("Type " + left.getType() + " Not allowed for Math Operation!");
            }
            return left.getType();
        }
        else{
            throw new JottTypeMismatchException("Types " + left.getType() + " and " + right.getType() + " Do not match!");
        }
    }
}
