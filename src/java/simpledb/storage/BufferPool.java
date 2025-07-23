package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.print.attribute.standard.PageRanges;
import javax.xml.crypto.Data;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    public int numPages;
    private final Map<Integer, Page> bufferPool;
    private final LockManager lockManager = new LockManager();


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.bufferPool = Collections.synchronizedMap(new LinkedHashMap<Integer, Page>(numPages, 0.75f, true));
        // some code goes here
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
 public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        lockManager.acquireLock(tid, pid, perm); // 🔐 acquire lock first

        synchronized (bufferPool) {
            Page p = bufferPool.get(pid.hashCode());
            if (p != null) {
                return p;
            }

            if (bufferPool.size() >= numPages) {
                evictPage();
            }

            try {
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                bufferPool.put(pid.hashCode(), page);
                return page;
            } catch (Exception e) {
                throw new DbException("Failed to load page from disk: " + e.getMessage());
            }
        }
    }
       

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
   public void transactionComplete(TransactionId tid, boolean commit) {
    synchronized (this) {
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // Create a copy of pages to revert to avoid concurrent modification
            List<PageId> pagesToRevert = new ArrayList<>();
            for (Page page : bufferPool.values()) {
                if (tid.equals(page.isDirty())) {
                    pagesToRevert.add(page.getId());
                }
            }
            
            for (PageId pid : pagesToRevert) {
                try {
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page freshPage = dbFile.readPage(pid);
                    bufferPool.put(pid.hashCode(), freshPage);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
        // Release locks after all page operations are complete
        lockManager.releaseAllLocks(tid);
    }
}
    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        try {
            List<Page> pageList = hf.insertTuple(tid, t); // list of dirty pages
            for (Page p : pageList) { // cache them into bufferpool
                p.markDirty(true, tid);
                this.bufferPool.put(p.getId().hashCode(), p);
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = hf.deleteTuple(tid, t);
        for (Page p : pageList) { // cache them into bufferpool
            p.markDirty(true, tid);
            this.bufferPool.put(p.getId().hashCode(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Page[] pages;
        synchronized (bufferPool) {
            pages = bufferPool.values().toArray(new Page[0]);
        }

        // Flush each page (no iteration over LinkedHashMap)
        for (Page p : pages) {
            if (p != null && p.isDirty() != null) {
                flushPage(p.getId());
            }
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferPool.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (!bufferPool.containsKey(pid.hashCode())) {
            return;
        }
        Page p = bufferPool.get(pid.hashCode());
        if (p.isDirty() != null) {
            DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
            df.writePage(p);
            p.markDirty(false, null);
        }

    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page : new ArrayList<>(bufferPool.values())) {
            TransactionId dirtier = page.isDirty();
            if (dirtier != null && dirtier.equals(tid)) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //NOW DOING NO STEAL - CANNOT EVICT DIRTY PAGES
    
for (Map.Entry<Integer, Page> entry : bufferPool.entrySet()) {
            if (entry.getValue().isDirty() == null) {
                lockManager.releaseAllLocksOnPage(entry.getValue().getId());
                discardPage(entry.getValue().getId());
                return;
            }
        }
        throw new DbException("All pages are dirty. Cannot evict.");
    }


}