package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op op;
    private Map<Field,Integer> counts = new HashMap<>();
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        if(what != Op.COUNT){
            throw new IllegalArgumentException();
        }
        this.op = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field grouping = (gbfield == NO_GROUPING) ? null : tup.getField(this.gbfield);
        int currentCount = counts.getOrDefault(grouping, 0);
        counts.put(grouping, currentCount + 1);   
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> result = new ArrayList<>();
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
            Field group = null;
            int aggvalue = counts.getOrDefault(group,0);
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(aggvalue));
            result.add(t);

        } else {
            td = new TupleDesc(new Type[] { this.gbfieldType, Type.INT_TYPE });
            for (Field group : counts.keySet()) {
                int aggvalue = counts.getOrDefault(group,0);;
                Tuple t = new Tuple(td);
                t.setField(0, group);
                t.setField(1, new IntField(aggvalue));
                result.add(t);
            }
        }
        return new OpIterator() {
            private Iterator<Tuple> iter;

            @Override
            public void open() {
                iter = result.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                // TODO Auto-generated method stub
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return iter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                iter = result.iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                // TODO Auto-generated method stub
                return td;
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub
                iter = null;
            }

        };
    }
}
