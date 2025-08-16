package simpledb.storage;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    public class LocksOnPage {

        private boolean exclusive; // determines if it is an READ_WRITE lock
        private HashMap<TransactionId, Boolean> tidLockHolder; //HashMap of tids holding a lock
        private int waitingWriters;

        public LocksOnPage() {
            exclusive = false;
            tidLockHolder = new HashMap<>();
            waitingWriters = 0;
        }

        public void acquireReadLock(TransactionId tid) {
            synchronized (this) {
                if (tidLockHolder.containsKey(tid)) {
                    return;
                }
                try {
                    while (exclusive || waitingWriters > 0) { // if currently written, or transactions are waiting
                        this.wait();
                    }
                    tidLockHolder.put(tid, false); // Read Lock
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // pause the thread
                    throw new RuntimeException("Thread interrupted when acquiring lock");
                }
            }
        }

        public void acquireReadWriteLock(TransactionId tid) {
            synchronized (this) {
                try {
                    if (tidLockHolder.getOrDefault(tid, false)) { // if already holding a lock
                        return;
                    }
                    waitingWriters++;
                    while (exclusive || tidLockHolder.size() > (tidLockHolder.containsKey(tid) ? 1 : 0)) {
                        this.wait();
                    }
                    waitingWriters--;
                    if (waitingWriters < 0) {
                        waitingWriters = 0;
                    }
                    exclusive = true;
                    tidLockHolder.put(tid, true); // upgrade from read to write or put in new write lock
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // pause the thread
                    throw new RuntimeException("Thread interrupted when acquiring lock");
                }
            }
        }

        public void unlockRead(TransactionId tid) {
            synchronized (this) {
                if (tidLockHolder.containsKey(tid) && !tidLockHolder.get(tid)) { // if getting a read lock
                    tidLockHolder.remove(tid);
                }
                this.notifyAll();
            }
        }

        public void unlockWrite(TransactionId tid) {
            synchronized (this) {
                if (tidLockHolder.containsKey(tid) && tidLockHolder.get(tid)) { // if getting a read lock
                    tidLockHolder.remove(tid);
                    exclusive = false;
                }
                this.notifyAll();
            }
        }

        public synchronized Set<TransactionId> lockholders() {
            return tidLockHolder.keySet();
        }

        public boolean isExclusive() {
            return exclusive;
        }

        public synchronized boolean canRead(TransactionId tid) {
            return tidLockHolder.containsKey(tid);
        }

        public synchronized boolean canReadWrite(TransactionId tid) {
            return tidLockHolder.containsKey(tid) && tidLockHolder.get(tid);
        }
    }

    private Map<PageId, LocksOnPage> locks;
    private Map<TransactionId, Set<PageId>> tidMap;
    private Map<TransactionId, HashSet<TransactionId>> dependencies; // each transaction maps to a set of
                                                                         // transactions it is waiting on

    public LockManager() {
        this.locks = new ConcurrentHashMap<>();
        this.tidMap = new ConcurrentHashMap<>();
        this.dependencies = new ConcurrentHashMap<>();
    }

    boolean hasCycle(TransactionId tid, PageId pid, Permissions perm) { // use DFS to detect cycles
        if (!getLock(pid).isExclusive() && perm == Permissions.READ_ONLY) {
            return false;
        }
        ArrayList<TransactionId> visitedList = new ArrayList<>();
        visitedList.add(tid);
        HashSet<TransactionId> holdersSet = new HashSet<>(getLockHoldersTID(pid));
        holdersSet.remove(tid);
        for (TransactionId holderTID : holdersSet) {
            if (checkWaiting(new HashMap<>(dependencies), holderTID, new ArrayList<>(visitedList))) {
                return true;
            }
        }
        return false;
    }

    //helper function to check if there are any other dependencies waiting on the lock
    private synchronized boolean checkWaiting(HashMap<TransactionId, HashSet<TransactionId>> waitForGCopy,
            TransactionId waiter, ArrayList<TransactionId> visitedList) {
        if (visitedList.contains(waiter)) {
            return true;
        }

        visitedList.add(waiter);
        HashSet<TransactionId> holdersSet = waitForGCopy.get(waiter);
        for (TransactionId holderTID : holdersSet) {
            if (checkWaiting(waitForGCopy, holderTID, new ArrayList<>(visitedList))) {
                return true;
            }
        }
        return false;
    }

    //add a new tid waiting and update the list of waiting tid
    public synchronized void addDependencies(TransactionId waiter, TransactionId waitOn) {
        HashSet<TransactionId> waitingOnSet = dependencies.getOrDefault(waiter, new HashSet<>());
        waitingOnSet.add(waitOn);
        dependencies.put(waiter, waitingOnSet);
    }

    // updates the dependency graph by removing the transaction tid
    void removeDependencies(TransactionId waiter, TransactionId holder) {
        if (dependencies.containsKey(waiter)) {
            HashSet<TransactionId> holdersSet = dependencies.getOrDefault(waiter, new HashSet<>());
            holdersSet.remove(holder);
            if (holdersSet.isEmpty()) {
                dependencies.remove(waiter);
            } else {
                dependencies.put(waiter, holdersSet);
            }
        }
    }

    //helper function to acquire read or write lock
    public void acquireLock(PageId pid, TransactionId tid, Permissions perm) {
        switch (perm) {
            case READ_ONLY:
                acquireReadLock(pid, tid);
                break;
            case READ_WRITE:
                acquireReadWriteLock(pid, tid);
                break;
        }
    }

    // Releases the lock on a page held by transaction tid
    public void releaseLock(PageId pid, TransactionId tid, Permissions perm) {
        switch (perm) {
            case READ_ONLY:
                releaseReadLock(pid, tid);
                break;
            case READ_WRITE:
                releaseReadWriteLock(pid, tid);
                break;
        }
    }

    public void acquireReadLock(PageId pid, TransactionId tid) {
        if (tidMap.containsKey(tid) && tidMap.get(tid).contains(pid)) {
            return;
        }
        if (!hasPageLock(pid, tid)) {
            LocksOnPage lock = getLockOrDefault(pid, tid);
            lock.acquireReadLock(tid);
        } else {
            LocksOnPage lock = locks.get(pid);
            if (!lock.canRead(tid)) {
                lock.acquireReadLock(tid);
            }
        }
        addToTIDMap(pid, tid);
    }

    public void acquireReadWriteLock(PageId pid, TransactionId tid) {
        if (!hasPageLock(pid, tid)) {
            LocksOnPage lock = getLockOrDefault(pid, tid);
            lock.acquireReadWriteLock(tid);
        } else {
            LocksOnPage lock = locks.get(pid);
            if (!lock.canReadWrite(tid)) {
                lock.acquireReadWriteLock(tid);
            }
        }
        addToTIDMap(pid, tid);
    }

    public void releaseReadLock(PageId pid, TransactionId tid) {
        LocksOnPage lock = locks.get(pid);
        lock.unlockRead(tid);
        removeFromTIDMap(pid, tid);
    }

    public void releaseReadWriteLock(PageId pid, TransactionId tid) {
        LocksOnPage lock = locks.get(pid);
        lock.unlockWrite(tid);
        removeFromTIDMap(pid, tid);
    }

    public LocksOnPage getLockOrDefault(PageId pid, TransactionId tid) {
        return locks.computeIfAbsent(pid, k -> new LocksOnPage());
    }

    public LocksOnPage getLock(PageId pid) {
        return locks.get(pid);
    }

    public void addToTIDMap(PageId pid, TransactionId tid) {
        Set<PageId> pgSet = getTransactionPIDs(tid);
        if (!pgSet.contains(pid)) {
            pgSet.add(pid);
        }
    }

    public void removeFromTIDMap(PageId pid, TransactionId tid) {
        if (tidMap.containsKey(tid)) {
            Set<PageId> pgSet = tidMap.get(tid);
            pgSet.remove(pid);
            if (pgSet.isEmpty()) {
                tidMap.remove(tid);
            }
        }
    }

    //add in a new hashset for a new tid
    public Set<PageId> getTransactionPIDs(TransactionId tid) {
        return tidMap.computeIfAbsent(tid, k -> new HashSet<>());
    }

    //check if the transction has a lock on the page id
    public boolean hasPageLock(PageId pid, TransactionId tid) {
        return getTransactionPIDs(tid).contains(pid);
    }

    //get all tids holding a lock on that pageid
    public HashSet<TransactionId> getLockHoldersTID(PageId pid) {
        HashSet<TransactionId> holders = new HashSet<>();
        for (HashMap.Entry<TransactionId, Set<PageId>> e : tidMap.entrySet()) {
            if (e.getValue().contains(pid)) {
                holders.add(e.getKey());
            }
        }
        return holders;
    }

    //check if the pid has a lock on it
    public boolean hasLock(PageId pid) {
        return locks.containsKey(pid);
    }

}