package simpledb.storage;

import java.io.*;
import java.util.*;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private List<PageId> LRUtracker;
    private List<Page> bufferpool;
    private int size = DEFAULT_PAGE_SIZE;
    private LockManager lockManager;

    /* map<waiterTID, awaiting PID> */
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        size = numPages;
        LRUtracker = new ArrayList<>(size);
        bufferpool = new ArrayList<>(size);
        lockManager = new LockManager();
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
     * Retrieve the specified page with the associated permissions. Will acquire
     * a lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is
     * present, it should be returned. If it is not present, it should be added
     * to the buffer pool and returned. If there is insufficient space in the
     * buffer pool, a page should be evicted and the new page should be added in
     * its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        Page page;
        // acquire lock
        synchronized (this) {
            try {
                HashSet<TransactionId> holdersSet = lockManager.getLockHoldersTID(pid);
                if (lockManager.hasLock(pid) && !lockManager.getLock(pid).canReadWrite(tid)) {
                    if (perm == Permissions.READ_WRITE || !lockManager.getLock(pid).canRead(tid)) {
                        if (lockManager.hasCycle(tid, pid, perm)) {
                            throw new TransactionAbortedException();
                        }
                    }
                }
                for (TransactionId holdersTID : holdersSet) {
                    lockManager.addDependencies(holdersTID, tid);
                }
                lockManager.acquireLock(pid, tid, perm);
                for (TransactionId holderTID : holdersSet) {
                    lockManager.removeDependencies(tid, holderTID);
                }
            } catch (Exception e) {
                throw new TransactionAbortedException();
            }

            if (LRUtracker.contains(pid)) {
                int pageIndex = LRUtracker.indexOf(pid);
                page = bufferpool.get(pageIndex);

                // update LRU: move to end
                LRUtracker.remove(pageIndex);
                bufferpool.remove(pageIndex);
                LRUtracker.add(pid);
                bufferpool.add(page);
            } else {
                while (bufferpool.size() >= size) {
                    evictPage();
                }
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                page = dbFile.readPage(pid);

                LRUtracker.add(pid);
                bufferpool.add(page);
            }
            // release lock in transaction complete
            return page;
        }
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result
     * in wrong behavior. Think hard about who needs to call this and why, and
     * why they can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

        lockManager.releaseLock(pid, tid, Permissions.READ_ONLY);
        lockManager.releaseLock(pid, tid, Permissions.READ_WRITE);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock Should ALWAYS
     *            commit, can just call trnsactionComplete(true)
     */
    public synchronized void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.hasPageLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

        synchronized (this) {
            try {
                if (commit) {
                    flushPages(tid);
                } else { //ABORT
                    for (int i = 0; i < bufferpool.size(); i++) {
                        Page page = bufferpool.get(i);
                        PageId pid = page.getId();
                        if (page.isDirty() != null && page.isDirty().equals(tid)) {
                            discardPage(pid);
                            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                            Page cleanPage = dbFile.readPage(pid);
                            putPage(cleanPage);
                        }
                    }
                }
            } catch (IOException | DbException e) {
                e.printStackTrace();
            }
            for (PageId pid : LRUtracker) {
                if (holdsLock(tid, pid)) {
                    unsafeReleasePage(tid, pid);
                }
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2). May
     * block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have been
     * dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = f.insertTuple(tid, t);
        for (Page p : dirtiedPages) {
            p.markDirty(true, tid);
            putPage(p);
        }
        // some code goes here
        // not necessary for lab1
    }


    //helper method to update all the lists involved
    public synchronized void putPage(Page p) throws DbException {
        PageId pid = p.getId();
        for (int i = 0; i < LRUtracker.size(); i++) {
            if (LRUtracker.get(i).equals(pid)) { // if page is in bufferpool, remove it and add the dirtied/updated one
                                                 // to
                // the tail of bufferpool (LRU is start of list)
                bufferpool.remove(i);
                LRUtracker.remove(i);
                bufferpool.add(p);
                LRUtracker.add(pid);
                return;
            }
        }

        if (bufferpool.size() >= this.size) { // if bufferpool is full, evict the LRU (head of list) using evictPage,
            // then add the dirtied page to bufferpool
            evictPage();
        }
        LRUtracker.add(pid);
        bufferpool.add(p);

    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write
     * lock on the page the tuple is removed from and any other pages that are
     * updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have been
     * dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        List<Page> dirtiedPages = f.deleteTuple(tid, t);
        for (Page p : dirtiedPages) {
            p.markDirty(true, tid);
            putPage(p);
        }
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it
     * writes dirty data to disk so will break simpledb if running in NO STEAL
     * mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : LRUtracker) {
            int ind = LRUtracker.indexOf(pid);
            if (ind != -1 && ind < bufferpool.size()) {
                for (Page p : bufferpool) {
                    if (p.isDirty() != null) {
                        flushPage(p.getId());
                    }
                }
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in
     * its cache.
     *
     * Also used by B+ tree files to ensure that deleted pages are removed from
     * the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        int ind = LRUtracker.indexOf(pid);

        Set<TransactionId> holders = lockManager.getLockHoldersTID(pid);
        for (TransactionId tid : holders) {
            unsafeReleasePage(tid, pid);
        }
        if (ind != -1 && ind < bufferpool.size()) {
            LRUtracker.remove(pid);
            bufferpool.remove(ind);
        }
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        int index = LRUtracker.indexOf(pid);
        if (index != -1) {
            Page page = bufferpool.get(index);
            if (page.isDirty() != null) {
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : LRUtracker) {
            Page page = bufferpool.get(LRUtracker.indexOf(pid));
            if (tid.equals(page.isDirty())) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private void evictPage() throws DbException {
        synchronized (this) {
            for (int i = 0; i < LRUtracker.size(); i++) {
                Page p = bufferpool.get(i);
                if (p.isDirty() == null) { // found a clean page
                    bufferpool.remove(i);
                    LRUtracker.remove(i);
                    return;
                }
            }
            // no clean page found
            throw new DbException("All pages are dirty; cannot evict");
        }
    }

}