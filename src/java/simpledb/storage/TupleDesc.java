package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tDItems.iterator();
    }

    private static final long serialVersionUID = 1L;
    public final ArrayList<TDItem> tDItems = new ArrayList<>();

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if(!(typeAr.length >0)){
            throw new ArrayIndexOutOfBoundsException("The array must contain at least one entry");
        }
        else{
            for (int idx = 0; idx < typeAr.length;idx++){
                tDItems.add(new TDItem(typeAr[idx], fieldAr[idx]));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        for (Type item: typeAr){
            tDItems.add(new TDItem(item, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tDItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i >= tDItems.size()){
            throw new NoSuchElementException("Invalid Index");
        }
        // some code goes here
        else{
            return tDItems.get(i).fieldName;
    }
}

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i >= tDItems.size()){
            throw new NoSuchElementException("Invalid Index");
        }
        // some code goes here
        else{
            return tDItems.get(i).fieldType;
    }
}

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        boolean result = false;
        for (int idx = 0;idx<tDItems.size();idx++){
            if(tDItems.get(idx).fieldName == null) throw new NoSuchElementException();
            if((tDItems.get(idx).fieldName.equals(name))){
                result = true;
                return idx;
            }
        }
        if(!result){
            throw new NoSuchElementException("Field with " + name+" does not exist");
        }
        return 0;
}

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(int idx=0;idx<tDItems.size();idx++){
            size += tDItems.get(idx).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<Type> typeArray = new ArrayList<>();
        ArrayList<String> nameArray = new ArrayList<>();

        for(TDItem item : td1.tDItems){
            typeArray.add(item.fieldType);
            nameArray.add(item.fieldName);
        }

        for(TDItem item : td2.tDItems){
            typeArray.add(item.fieldType);
            nameArray.add(item.fieldName);
        }
        
        
        return new TupleDesc(typeArray.toArray(new Type[0]),nameArray.toArray(new String[0]));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        TupleDesc other = (TupleDesc) o;
        if(this.tDItems.size()!= other.tDItems.size()) return false;
        for(int i = 0;i<tDItems.size();i++){
            if(!this.tDItems.get(i).fieldType.equals(other.tDItems.get(i).fieldType)){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String result = "";
        for (int idx = 0;idx < tDItems.size();idx++){
            result += getFieldType(idx);
            result += getFieldName(idx);
            result += ",";
    }
    return result;
}
}
