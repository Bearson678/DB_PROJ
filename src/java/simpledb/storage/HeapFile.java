package simpledb.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tD;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tD = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tD;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // some code goes here
        int pageNumber = pid.getPageNumber();
        long offset = pageNumber * BufferPool.getPageSize();

        ByteBuffer buffer = ByteBuffer.allocate(BufferPool.getPageSize());

        try(RandomAccessFile file = new RandomAccessFile(this.file, "r")){
        file.seek(offset);
        file.read(buffer.array());
        return new HeapPage((HeapPageId)pid, buffer.array());
        } catch (IllegalArgumentException e){
            throw new IllegalArgumentException("page does not exist in this file");
        }
        catch(Exception e){
            System.err.println(e);
            
        }
        return null;
            
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int fileOffset = page.getId().getPageNumber() * BufferPool.getPageSize();
        try {
            RandomAccessFile file = new RandomAccessFile(this.file, "rw");
            file.seek(fileOffset);
            file.write(page.getPageData());
            file.close();
        } catch (Exception e) {
            System.err.println(e);
            throw new IOException("write failed");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>(); // store modified page to return
        
        // track if page to insert tuple in has been found
        boolean found = false;

        
        HeapPageId pid;
        HeapPage page = null; // assign null so java doesn't complain

        // insert into curr avail pages with empty slots
        for (int i = 0; i < this.numPages(); i++){
            pid = new HeapPageId(this.getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); 

            if (page.getNumEmptySlots() > 0) { // add into empty slot if there is
                found = true;
                break;
            }      
        }
        
        if (found == false) { // make new page if all were full & not inserted yet
            pid = new HeapPageId(this.getId(), this.numPages());
            
            byte[] emptyPage = HeapPage.createEmptyPageData();

            // add new page to file
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")){
                raf.seek(file.length());
                raf.write(emptyPage); 
                raf.close();
            } catch(Exception e){
                System.err.println(e);
                throw new IOException("file cannot be written");
            }

            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        }

        try {
            page.insertTuple(t);
            page.markDirty(true, tid);
        } catch (Exception e) {
            throw new DbException("tuple could not be added");
        }
        
        modifiedPages.add(page);
        return modifiedPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>(); // store modified page to return

        RecordId recordId = t.getRecordId();
        
        if (recordId == null){
            throw new DbException("tuple cannot be deleted");
        }

        PageId pid = recordId.getPageId();
        
        if (pid.getTableId() != this.getId()){
            throw new DbException("tuple is not a member of the file");
        }

        

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        try {
            page.deleteTuple(t);
            page.markDirty(true, tid);
            modifiedPages.add(page);

        } catch (Exception e) {
            throw new DbException("tuple cannot be deleted");
        }
        
        return modifiedPages;
        
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator {
    	private HeapPage hp;
    	private Iterator<Tuple> it;
    	private TransactionId tid;
    	private int tableId;
    	private HeapFile hf;
    	
    	public HeapFileIterator(TransactionId tid, HeapFile hf) {
    		this.tableId = hf.getId();
    		this.tid = tid;
    		this.hf = hf;
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
    		HeapPageId pId = new HeapPageId(tableId, 0);
    		hp = (HeapPage) Database.getBufferPool().getPage(tid, pId, Permissions.READ_ONLY);
    		it = hp.iterator();
		}

		@Override
		public boolean hasNext() {
			if(hp == null)
				return false;

			if(it.hasNext())
				return true;
            int currPNo = hp.getId().getPageNumber();
            currPNo++;
			while(currPNo < hf.numPages()) {
				try {
					HeapPageId pId = new HeapPageId(tableId, currPNo);
					hp = (HeapPage) Database.getBufferPool().getPage(tid, pId, Permissions.READ_ONLY);
					it = hp.iterator();
					if(it.hasNext())return true;

                    currPNo++;
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
			}

			return false;
		}

		@Override
		public Tuple next() throws NoSuchElementException {
			if(this.hasNext() ==  false)
				throw new NoSuchElementException();

			if(it.hasNext())
				return it.next();
			else
				return null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			open();
		}

		@Override
		public void close() {
			hp = null;
			it = null;
			tid = null;
			hf = null;
		}
    	
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }
    
}

