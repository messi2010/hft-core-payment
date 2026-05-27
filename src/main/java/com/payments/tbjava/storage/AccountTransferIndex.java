package com.payments.tbjava.storage;

import com.payments.tbjava.domain.UInt128;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * Secondary index: {@code accountId -> transfer ids touching it}, in insertion
 * (timestamp) order. Backs {@code get_account_transfers}, which would otherwise
 * require a full scan of the transfer store.
 *
 * <p>Each transfer is recorded under both its debit and its credit account.
 * NOT thread-safe -- owned by the single writer thread like the primary stores.
 * Entries are removed on linked-chain rollback (LIFO within a chain), so the
 * index never references a transfer that was undone.
 */
public final class AccountTransferIndex {

    private final Object2ObjectOpenHashMap<UInt128, ObjectArrayList<UInt128>> byAccount =
            new Object2ObjectOpenHashMap<>();

    public void add(UInt128 accountId, UInt128 transferId) {
        byAccount.computeIfAbsent(accountId, k -> new ObjectArrayList<>()).add(transferId);
    }

    /** Remove the most recent occurrence of {@code transferId} for the account (chain rollback). */
    public void remove(UInt128 accountId, UInt128 transferId) {
        ObjectArrayList<UInt128> list = byAccount.get(accountId);
        if (list == null) return;
        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).equals(transferId)) {
                list.remove(i);
                break;
            }
        }
        if (list.isEmpty()) byAccount.remove(accountId);
    }

    /** Most recent {@code limit} transfer ids for the account, newest first. */
    public UInt128[] recent(UInt128 accountId, int limit) {
        ObjectArrayList<UInt128> list = byAccount.get(accountId);
        if (list == null || limit <= 0) return new UInt128[0];
        int n = Math.min(limit, list.size());
        UInt128[] out = new UInt128[n];
        for (int i = 0; i < n; i++) {
            out[i] = list.get(list.size() - 1 - i);
        }
        return out;
    }

    public int count(UInt128 accountId) {
        ObjectArrayList<UInt128> list = byAccount.get(accountId);
        return list == null ? 0 : list.size();
    }
}
