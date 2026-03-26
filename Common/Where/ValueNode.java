package Common.Where;

import AttributeInfo.AttributeTypeEnum;
import Catalog.TableSchema;

import java.util.List;

public class ValueNode implements IOperandNode{

    private Object value;
    private AttributeTypeEnum type;

    public ValueNode(Object value, AttributeTypeEnum type){
        this.value = value;
        this.type = type;
    }

    @Override
    public Object getValue(List<Object> tuple, TableSchema tableSchema) {
        if (type == AttributeTypeEnum.INTEGER){
            return Integer.parseInt((String) value);
        }
        else if (type == AttributeTypeEnum.DOUBLE){
            return Double.parseDouble((String) value);
        }
        else if (type == AttributeTypeEnum.BOOLEAN){
            return Boolean.parseBoolean((String) value);
        }
        return value;
    }

    @Override
    public AttributeTypeEnum getType() {
        return type;
    }
}
