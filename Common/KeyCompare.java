package Common;

import java.util.Comparator;

public class KeyCompare implements Comparator<Object> {
    public int compare(Object searchKey1, Object searchKey2) {
        return ((Comparable)searchKey1).compareTo(searchKey2);
    }
}
