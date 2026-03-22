package Common.Where;

import Catalog.TableSchema;

import java.util.List;

public class AttributeNode implements IOperandNode{


    private String attributeName;

    public AttributeNode(String attributeName){
        this.attributeName = attributeName;
    }
    @Override
    public Object getValue(List<Object> tuple, TableSchema tableSchema) {
        //todo look up att name
        return null;
    }
}
