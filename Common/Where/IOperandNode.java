package Common.Where;

import AttributeInfo.AttributeTypeEnum;
import Catalog.TableSchema;

import java.util.List;

public interface IOperandNode {
    public Object getValue(List<Object> tuple, TableSchema tableSchema);

    public AttributeTypeEnum getType();
}
