package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private Boolean called = false;
    public Insert(TransactionId t, OpIterator child, int tableId)
    
            throws DbException {
        // some code goes here
        this.t =t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"inserted_rows"});

        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("tupledesc of child differs from table into which we are to insert");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //inserts tuples from the child only once then return a single tuple to show
                                                                                // How many rows were inserted
                                                                                //Need boolean flag if we keep calling the fetchNext, else we will insert more tuples again
      if(called) return null;
      called = true;
      
      int count = 0;
      BufferPool bufferpool = Database.getBufferPool();

      try {
        while(child.hasNext()){
            Tuple nextTuple = child.next();
            try{
                bufferpool.insertTuple(t, tableId, nextTuple);
                count++;
            } catch (TransactionAbortedException e) {
                throw e; //need to throw transactionAbortedException if it occurs
            }catch(IOException e) {
                throw new DbException("IO Exception while inserting tuple: " + e.getMessage());
            }
        }
      } catch (TransactionAbortedException e) {
        throw e;
      } catch (DbException e) {
        throw e;
      }
      Tuple resultTuple = new Tuple(this.getTupleDesc());
      resultTuple.setField(0, new IntField(count));
      return resultTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Filter requires exactly one child.");
        }
        this.child = children[0];
    }
}