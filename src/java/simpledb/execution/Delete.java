package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private Boolean called = false;
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t = t;
        this.child =child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"deleted_rows"});
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(called)return null;
        called = true;
        int count = 0;
        BufferPool bp = Database.getBufferPool();
        try {
            while (child.hasNext()){
            Tuple nextTuple = child.next();
            try {
            bp.deleteTuple(t, nextTuple);
            count++;                
            } catch (TransactionAbortedException e) {
                throw e;  //need to throw transactionAbortedException if it occurs
            } catch (IOException e) {
                throw new DbException("IO error during tuple deletion: " + e.getMessage());
            } catch (DbException e) {
                throw e;
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
