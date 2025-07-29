package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;

public class LockManager {

    public class LocksOnPage {

        private TransactionId exclusiveLock; // For READ_WRITE locks
        private Set<TransactionId> sharedLocks; // For READ_ONLY locks

        public LocksOnPage() {
            this.sharedLocks = new HashSet<>();
            this.exclusiveLock = null;
        }

        public void addSharedLock(TransactionId tid) {
            this.sharedLocks.add(tid);
        }

        public void removeSharedLock(TransactionId tid) {
            this.sharedLocks.remove(tid);
        }

        public void removeExclusiveLock(TransactionId tid) {
            if (tid.equals(exclusiveLock)) {
                exclusiveLock = null;
            }
        }

        public void setExclusiveLock(TransactionId tid) {
            this.exclusiveLock = tid;
        }

        public boolean exclusivelyLocked() {
            return this.exclusiveLock != null;
        }

        public boolean holdsSharedLock(TransactionId tid) {
            return this.sharedLocks.contains(tid);
        }

        public boolean holdsExclusiveLock(TransactionId tid) {
            if (this.exclusiveLock != null) {
                return this.exclusiveLock.equals(tid);
            }
            return false;
        }

        public boolean isLocked() {
            return this.exclusiveLock != null || this.sharedLocks.size() != 0;
        }
    }

    private Map<PageId, LocksOnPage> locks;
    private Map<TransactionId, Set<TransactionId>> dependencies; // each transaction maps to a set of transactions it is waiting on

    public LockManager() {
        this.locks = new HashMap<>();
        this.dependencies = new HashMap<>();
    }

    // Checks if there are any deadlocks relating to transaction tid
    public synchronized boolean deadlocked(TransactionId tid) {
        if (!this.dependencies.containsKey(tid)) {
            return false;
        }
        
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> stack = new HashSet<>();
        return hasCycle(tid, visited, stack);
    }

    private boolean hasCycle(TransactionId tid, Set<TransactionId> visited, Set<TransactionId> stack) { //use DFS to detect cycles
        if (stack.contains(tid)) {
            return true; // Found a cycle
        }
        if (visited.contains(tid)) {
            return false; 
        }
        
        visited.add(tid);
        stack.add(tid);
        
        if (dependencies.containsKey(tid)) {
            for (TransactionId dependency : dependencies.get(tid)) {
                if (hasCycle(dependency, visited, stack)) {
                    return true;
                }
            }
        }
        
        stack.remove(tid);
        return false;
    }

    //updates the dependency graph by removing the transaction tid
    private void removeDependencies(TransactionId tid) {
        dependencies.remove(tid);
        
        // Remove this transaction from other transactions' dependency lists
        for (Set<TransactionId> deps : dependencies.values()) {
            deps.remove(tid);
        }
    }

    public void acquire(TransactionId tid, PageId pid, Permissions perms) //acquires a lock on a particular page
            throws TransactionAbortedException {

        while (true) {
            synchronized (this) {
                LocksOnPage lock = locks.getOrDefault(pid, new LocksOnPage());
                
                // if requesting shared lock
                if (perms == Permissions.READ_ONLY) {
                    // Can acquire shared lock if: no exlusive lock or already holds exclusive lock
                    if (!lock.exclusivelyLocked() || lock.holdsExclusiveLock(tid)) {
                        removeDependencies(tid); 
                        lock.addSharedLock(tid); //update the shared locks
                        locks.put(pid, lock); //update the locks map
                        return; 
                    } else {
                        dependencies.computeIfAbsent(tid, k -> new HashSet<>())
                                   .add(lock.exclusiveLock); // add the exclusive lock holder to dependencies, means the calling transaction must wait
                    }
                } 
                // if requesting exclusive lock
                else {
                    if (lock.holdsExclusiveLock(tid)) {
                        removeDependencies(tid);
                        return; 
                    }
                    
                    
                    if (!lock.isLocked()) { // No locks on this page, can acquire exclusive lock
                        removeDependencies(tid);
                        lock.setExclusiveLock(tid);
                        locks.put(pid, lock);
                        return; 
                    }
                    
                    // handles lock upgrade from read to write
                    if (lock.holdsSharedLock(tid) && lock.sharedLocks.size() == 1 && !lock.exclusivelyLocked()) {
                        removeDependencies(tid);
                        lock.removeSharedLock(tid);
                        lock.setExclusiveLock(tid);
                        locks.put(pid, lock);
                        return; 
                    } else {
                        // Must wait for all current lock holders
                        Set<TransactionId> waitFor = dependencies.computeIfAbsent(tid, k -> new HashSet<>());
                        
                        if (lock.exclusivelyLocked()) {
                            waitFor.add(lock.exclusiveLock);
                        } else {
                            // Wait for all shared lock holders (except ourselves)
                            for (TransactionId sharedHolder : lock.sharedLocks) {
                                if (!sharedHolder.equals(tid)) {
                                    waitFor.add(sharedHolder);
                                }
                            }
                        }
                    }
                }
                
                // Check for deadlock after updating dependencies
                if (deadlocked(tid)) {
                    removeDependencies(tid); // if deadlocked, remove dependencies
                    System.out.println("Deadlock detected for transaction " + tid);
                    throw new TransactionAbortedException();
                }
            }
            
        }
    }

    // Releases the lock on a page held by transaction tid
    public synchronized void release(TransactionId tid, PageId pid) {
        if (!locks.containsKey(pid)) {
            return; // No locks on this page
        }
        
        LocksOnPage lock = locks.get(pid);
        
        if (lock.holdsSharedLock(tid)) {
            lock.removeSharedLock(tid);
        }
        
        if (lock.holdsExclusiveLock(tid)) {
            lock.removeExclusiveLock(tid);
        }
        
        if (lock.sharedLocks.isEmpty() && !lock.exclusivelyLocked()) {
            locks.remove(pid);
        }
        
        removeDependencies(tid); //after releasing the lock, remove the transaction from dependencies
        notifyAll(); //wake up any waiting threads
    }

    //checks if any transaction holds a lock on the page
    public synchronized boolean isLocked(PageId pid) {
        LocksOnPage lock = locks.get(pid);
        return lock != null && lock.isLocked();
    }

    // check if the transaction tid holds a lock on the page pid
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        LocksOnPage lock = locks.get(pid);
        if (lock == null) {
            return false;
        }
        return lock.holdsExclusiveLock(tid) || lock.holdsSharedLock(tid);
    }

    // Removes all locks held by transaction 
    public synchronized void removeAllHeld(TransactionId tid) {
        List<PageId> toRelease = new ArrayList<>();
        
        // Find all pages where this transaction holds locks
        for (Map.Entry<PageId, LocksOnPage> entry : locks.entrySet()) {
            PageId pid = entry.getKey();
            LocksOnPage lock = entry.getValue();
            
            if (lock.holdsExclusiveLock(tid) || lock.holdsSharedLock(tid)) {
                toRelease.add(pid);
            }
        }
        
        // Release all locks found
        for (PageId pid : toRelease) {
            release(tid, pid);
        }
    }


    // Release all locks on a specific page
    public synchronized void releaseAllLocksOnPage(PageId pid) {
        if (!locks.containsKey(pid)) {
            return;
        }
        
        LocksOnPage lock = locks.get(pid);
        
        // Get all transactions that hold locks on this page
        Set<TransactionId> lockHolders = new HashSet<>();
        lockHolders.addAll(lock.sharedLocks);
        if (lock.exclusivelyLocked()) {
            lockHolders.add(lock.exclusiveLock);
        }
        
        // Remove the page from locks
        locks.remove(pid);
        
        // release all locks held by these transactions on that particular page
        for (TransactionId tid : lockHolders) {
            release(tid, pid);
        }
        
    }

}