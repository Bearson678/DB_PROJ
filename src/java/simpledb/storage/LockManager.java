package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    class Lock {
        Set<TransactionId> lockHolders = new HashSet<>();
        Permissions perm;

        Lock(TransactionId tid, Permissions perm) {
            this.lockHolders.add(tid);
            this.perm = perm;
        }

        boolean isCompatible(TransactionId tid, Permissions newPerm) {
            if (lockHolders.contains(tid))
                return true;
            if (perm == Permissions.READ_ONLY && newPerm == Permissions.READ_ONLY)
                return true;
            return false;
        }

    }

    private Map<PageId, Lock> lockMap = new ConcurrentHashMap<>();
    private Map<TransactionId, Set<PageId>> transacLocks = new ConcurrentHashMap<>();

    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        long timeout = System.currentTimeMillis() + 1000;

        while (true) {
            Lock currentLock = lockMap.get(pid);
            if (currentLock == null || currentLock.isCompatible(tid, perm)) {
                Lock newLock = new Lock(tid, perm);
                if (currentLock != null) {
                    newLock.lockHolders.addAll(currentLock.lockHolders);
                }
                newLock.lockHolders.add(tid);
                newLock.perm = (currentLock == null || perm == Permissions.READ_WRITE) ? perm : currentLock.perm;
                lockMap.put(pid, newLock);
                transacLocks.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
                return true;

            }

            if (System.currentTimeMillis() > timeout) {
                throw new TransactionAbortedException();
            }

            try {
                wait(50); // sleep briefly
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        Lock currentLock = lockMap.get(pid);
        if (currentLock != null && currentLock.lockHolders.contains(tid)) {
            currentLock.lockHolders.remove(tid);
            if (currentLock.lockHolders.isEmpty()) {
                lockMap.remove(pid);
            }
            Set<PageId> pids = transacLocks.get(tid);
            if (pids != null) {
                pids.remove(pid);
                if (pids.isEmpty()) {
                    transacLocks.remove(tid);
                }
            }
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Lock currentLock = lockMap.get(pid);
        return currentLock != null && currentLock.lockHolders.contains(tid);
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pids = transacLocks.remove(tid);
        if (pids != null) {
            for (PageId pid : pids) {
                Lock currentLock = lockMap.get(pid);
                if (currentLock != null) {
                    currentLock.lockHolders.remove(tid);
                    if (currentLock.lockHolders.isEmpty()) {
                        lockMap.remove(pid);
                    }
                }
            }
        }
        notifyAll();

    }

}
