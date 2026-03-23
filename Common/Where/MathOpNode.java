package Common.Where;

import Catalog.TableSchema;

import java.util.List;

public class MathOpNode implements IOperandNode{
    private IOperandNode left;
    private IOperandNode right;
    private MathOp operation;
    public MathOpNode(IOperandNode left, IOperandNode right, String op){
        this.left = left;
        this.right = right;
        operation = MathOp.ADD;
        //todo figure out operation from string
    }


    @Override
    public Object getValue(List<Object> tuple, TableSchema tableSchema) {
        Object valLeft = left.getValue(tuple, tableSchema);
        Object valRight = right.getValue(tuple,tableSchema);
        //todo cast these into proper typing and do math op



        return null;
    }
}
