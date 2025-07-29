package simpledb.storage;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

import javax.xml.crypto.Data;

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
        long offset = (long) pageNumber * BufferPool.getPageSize();

        ByteBuffer buffer = ByteBuffer.allocate(BufferPool.getPageSize());

        try (RandomAccessFile raf = new RandomAccessFile(this.file, "r")) {
            raf.seek(offset);
            int bytesRead = raf.read(buffer.array());
            if (bytesRead < BufferPool.getPageSize()) {
        
                throw new IllegalArgumentException("hp does not exist in this file");
            }
            return new HeapPage((HeapPageId) pid, buffer.array());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("hp does not exist in this file");
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException("Failed to read hp", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page hp) throws IOException {
        // some code goes here
        // not necessary for lab1
        int fileOffset = hp.getId().getPageNumber() * BufferPool.getPageSize();
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            raf.seek(fileOffset);
            raf.write(hp.getPageData());
            raf.close();
        } catch (Exception e) {
            System.err.println(e);
            throw new IOException("write failed", e);
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
        ArrayList<Page> pages = new ArrayList<>();
        HeapPageId pid;
        HeapPage hp = null;
        boolean found = false;
        
        int numPages = this.numPages();
        for (int i = 0; i < numPages; i++) {
            pid = new HeapPageId(this.getId(), i);
            hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY); //Get the hp first
            if (hp.getNumEmptySlots() > 0) {//if the hp has empty slots, insert the tuple
                Database.getBufferPool().unsafeReleasePage(tid, pid); // release the read-only lock
                hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); //Get the hp again with write lock
                if(hp.getNumEmptySlots() > 0) {
                    found = true;
                    break; // exit the loop once we find a hp with space
                }else{
                    Database.getBufferPool().unsafeReleasePage(tid, pid); // release the write lock if no space
                }
            }else{
                Database.getBufferPool().unsafeReleasePage(tid, pid); // release the read-only lock if no space
            }
        }
        //if no existing hp has space,create a new hp
        if(!found){
            synchronized (this) {
                for(int i =0;i<numPages;i++){
                    pid = new HeapPageId(this.getId(), i);
                    hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    if(hp.getNumEmptySlots() > 0){
                        Database.getBufferPool().unsafeReleasePage(tid, pid);
                        hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                        if(hp.getNumEmptySlots() > 0){
                            found = true;
                            break; // exit the loop once we find a hp with space
                        }else{
                            Database.getBufferPool().unsafeReleasePage(tid, pid);
                        }
                    }else{
                        Database.getBufferPool().unsafeReleasePage(tid, pid);
                    }
                }

                if (!found) {
                    int newPageNumber = this.numPages();
                    pid = new HeapPageId(this.getId(), newPageNumber);

                    byte[] emptyPage = HeapPage.createEmptyPageData();
                    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                        raf.seek(file.length());
                        raf.write(emptyPage);
                    } catch (Exception e) {
                        System.err.println(e);
                        throw new IOException("file cannot be written", e);
                    }
                    
                    // Get the newly created hp with READ_WRITE permissions
                    hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                    found = true;
                }
            }
        }

        // Insert the tuple
        try {
            hp.insertTuple(t);
            hp.markDirty(true, tid);
        } catch (Exception e) {
            throw new DbException("tuple could not be added");
        }
        
        pages.add(hp);
        return pages;
    }
            
        // not necessary for lab1


    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
ArrayList<Page> modifiedPages = new ArrayList<>();

        RecordId recordId = t.getRecordId();
        
        if (recordId == null) {
            throw new DbException("tuple cannot be deleted - no record ID");
        }

        PageId pid = recordId.getPageId();
        
        if (pid.getTableId() != this.getId()) {
            throw new DbException("tuple is not a member of this file");
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
    }
        // not necessary for lab1
    

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