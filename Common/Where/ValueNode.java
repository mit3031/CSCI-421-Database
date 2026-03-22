package Common.Where;

import Catalog.TableSchema;

import java.util.List;

public class ValueNode implements IOperandNode{

    private Object value;

    public ValueNode(Object value){
        this.value = value;
    }

    @Override
    public Object getValue(List<Object> tuple, TableSchema tableSchema) {
        return value;
    }
}
