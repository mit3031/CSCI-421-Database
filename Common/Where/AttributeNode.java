package Common.Where;

import AttributeInfo.Attribute;
import AttributeInfo.AttributeTypeEnum;
import Catalog.TableSchema;

import java.util.List;

public class AttributeNode implements IOperandNode{


    private String attributeName;
    private AttributeTypeEnum type;
    /**
     * Creates an Attribute node
     * @param attributeName Name of the attribute
     * @param tableSchema Schema to find the type of the attribute in
     */
    public AttributeNode(String attributeName, TableSchema tableSchema)
    {
        this.attributeName = attributeName;
        boolean foundUnqualified = false;
        boolean dupeUnqualified = false;
        boolean foundQualified = false;
        Attribute temp = null;
        for (Attribute a : tableSchema.getAttributes()){
            String name = a.getName();
            String unqualifiedName = name;
            if(unqualifiedName.contains(".")){
                unqualifiedName = unqualifiedName.substring(unqualifiedName.indexOf(".") + 1);
            }
            //qualified name guaranteed to be unique
            if(name.equals(attributeName)){
                foundQualified = true;
                temp = a;
                break;
            }
            if(unqualifiedName.equals(attributeName)){
                // two attributes, when unqualified become issue
                dupeUnqualified = foundUnqualified;
                foundUnqualified = true;
                temp = a;
            }
        }
        if (temp == null){
            throw new JottUnfoundAttributeException("Attribute name " + attributeName + " Not Found!");
        }
        //check if there was a duplicate unqualified And no qualified found
        if (!foundQualified && dupeUnqualified){
            throw new JottAmbiguousNameException("Attribute name " + attributeName + " Is ambiguous!");
        }
        type = temp.getDefinition().getType();

    }
    @Override
    public Object getValue(List<Object> tuple, TableSchema tableSchema) {
        boolean foundUnqualified = false;
        boolean dupeUnqualified = false;
        boolean foundQualified = false;
        //index which we need to get from tuple
        int attributeIndex = -1;
        for (int i = 0; i < tableSchema.getAttributes().size(); i++){
            Attribute a = tableSchema.getAttributes().get(i);
            String name = a.getName();
            String unqualifiedName = name;
            if(name.contains(".")){
                unqualifiedName = unqualifiedName.substring(unqualifiedName.indexOf(",")+1);
            }
            //first check qualified name
            if(name.equals(attributeName)){
                foundQualified = true;
                attributeIndex = i;
                break;
            }
            //unqualified does not break loop because need to check duplicate, or qualified match could be later
            else if(unqualifiedName.equals(attributeName)) {
                dupeUnqualified = foundUnqualified;
                foundUnqualified = true;
                attributeIndex = i;
            }
        }
        if(attributeIndex == -1){
            throw new JottUnfoundAttributeException("Attribute name " + attributeName + " Not Found!");
        }
        if(!foundQualified && dupeUnqualified){
            throw new JottAmbiguousNameException("Attribute name " + attributeName + " Is ambiguous!");
        }

        //now get index that should be the right attribute
        return tuple.get(attributeIndex);

    }

    @Override
    public AttributeTypeEnum getType() {
        return type;
    }
}
