package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    
    private final int numPages;
    private final Map<PageId, Page> bufferPool;
    private final LockManager lockManager = new LockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.bufferPool = new ConcurrentHashMap<PageId, Page>();
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

        lockManager.acquire(tid, pid, perm); //acquire the lock first
        
        try {
            // Check if page is already in buffer pool
            Page page = bufferPool.get(pid);
            if (page != null) {
                return page;
            }

            // Page not in buffer - need to load from disk
            Page pageToLoad = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            
            synchronized (this) {
                // Double-check in case another thread loaded the page
                Page existingPage = bufferPool.get(pid);
                if (existingPage != null) {
                    return existingPage;
                }
                
                if (bufferPool.size() >= numPages) {
                    evictPage();
                }
                
                bufferPool.put(pid, pageToLoad);
                return pageToLoad;
            }
            
        } catch (Exception e) {
            // If anything goes wrong, release the lock we just acquired
            lockManager.release(tid, pid);
            if (e instanceof TransactionAbortedException) {
                throw (TransactionAbortedException) e;
            } else if (e instanceof DbException) {
                throw (DbException) e;
            } else {
                throw new DbException("Failed to get page: " + e.getMessage());
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
        lockManager.release(tid, pid);
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
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        try {
            if (commit) {
                
                flushPages(tid);
            } else {
                revertPages(tid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lockManager.removeAllHeld(tid);
        }
    }

    /**
     * Reverts all pages dirtied by the specified transaction by reloading them
     * from disk. This is used when a transaction aborts to ensure that no dirty
     * pages remain in the buffer pool.
     *
     * @param tid the ID of the transaction to revert
     */
    private void revertPages(TransactionId tid) {
        List<PageId> pagesToRevert = new ArrayList<>();
        
        for (Page page : bufferPool.values()) {
            if (tid.equals(page.isDirty())) {
                pagesToRevert.add(page.getId());
            }
        }
        
        // Revert each page by reloading from disk
        for (PageId pid : pagesToRevert) {
            try {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page freshPage = dbFile.readPage(pid);
                bufferPool.put(pid, freshPage);
            } catch (Exception e) {
                e.printStackTrace();
                // Continue reverting other pages even if one fails
            }
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
        List<Page> pageList = hf.insertTuple(tid, t); // list of dirty pages
        
        // Cache dirty pages in buffer pool
        for (Page page : pageList) {
            page.markDirty(true, tid);
            bufferPool.put(page.getId(), page);
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
        
        // Cache dirty pages in buffer pool
        for (Page page : pageList) {
            page.markDirty(true, tid); 
            bufferPool.put(page.getId(), page); //add the dirty page to the buffer pool
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
        // create a copy of the buffer pool to avoid concurrent modification
        List<Page> pages = new ArrayList<>(bufferPool.values());

        // Flush each dirty page
        for (Page page : pages) {
            if (page != null && page.isDirty() != null) {
                flushPage(page.getId());
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
        bufferPool.remove(pid);
        lockManager.releaseAllLocksOnPage(pid); // Release locks on this page
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1        
        Page page = bufferPool.get(pid);
        if (page != null && page.isDirty() != null) {
            HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            hf.writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
       
        List<Page> pagesToFlush = new ArrayList<>();
        
        for (Page page : bufferPool.values()) {
            TransactionId dirtier = page.isDirty();
            if (dirtier != null && dirtier.equals(tid)) {
                pagesToFlush.add(page);
            }
        }
        
       
        for (Page page : pagesToFlush) {
            flushPage(page.getId());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Uses simple policy for now - just remove first clean page found
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //NOW DOING NO STEAL - CANNOT EVICT DIRTY PAGES        
        // Find first clean page to evict (NO STEAL policy)
        for (Map.Entry<PageId, Page> entry : bufferPool.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() == null) {
                // Found a clean page - safe to evict
                PageId pid = entry.getKey();
                bufferPool.remove(pid);
                lockManager.releaseAllLocksOnPage(pid); // Release locks on this page
                return;
            }
        }
        
        // All pages are dirty - cannot evict under NO STEAL policy
        throw new DbException("All pages in buffer pool are dirty. Cannot evict any page under NO STEAL policy.");
    }
}