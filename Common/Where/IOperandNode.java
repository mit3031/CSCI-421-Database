package Common.Where;

import Catalog.TableSchema;

import java.util.List;

public interface IOperandNode {
    public Object getValue(List<Object> tuple, TableSchema tableSchema);
}
