package peergos.shared.storage;

import peergos.shared.*;
import peergos.shared.cbor.*;
import peergos.shared.corenode.*;
import peergos.shared.crypto.*;
import peergos.shared.crypto.hash.*;
import peergos.shared.hamt.*;
import peergos.shared.io.ipfs.cid.*;
import peergos.shared.io.ipfs.multihash.*;
import peergos.shared.mutable.*;
import peergos.shared.storage.auth.*;
import peergos.shared.user.*;
import peergos.shared.user.fs.*;
import peergos.shared.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class BufferedStorage extends DelegatingStorage {

    private Map<Cid, OpLog.BlockWrite> storage = new LinkedHashMap<>();
    private final ContentAddressedStorage target;
    private final Hasher hasher;

    public BufferedStorage(ContentAddressedStorage target, Hasher hasher) {
        super(target);
        if (target instanceof BufferedStorage)
            throw new IllegalStateException("Nested BufferedStorage!");
        this.target = target;
        this.hasher = hasher;
    }

    public boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public ContentAddressedStorage directToOrigin() {
        return this;
    }

    @Override
    public CompletableFuture<TransactionId> startTransaction(PublicKeyHash owner) {
        TransactionId tid = new TransactionId(Long.toString(System.currentTimeMillis()));
        return CompletableFuture.completedFuture(tid);
    }

    @Override
    public CompletableFuture<Boolean> closeTransaction(PublicKeyHash owner, TransactionId tid) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<List<byte[]>> getChampLookup(PublicKeyHash owner, Cid root, byte[] champKey, Optional<BatWithId> bat) {
        if (storage.isEmpty())
            return target.getChampLookup(owner, root, champKey, bat);
        // If we are in a write transaction try to perform a local champ lookup from the buffer,
        // falling back to a direct champ get
        return Futures.asyncExceptionally(
                () -> getChampLookup(root, champKey, bat, hasher),
                t -> target.getChampLookup(owner, root, champKey, bat)
        );
    }

    @Override
    public CompletableFuture<List<byte[]>> getChampLookup(Cid root, byte[] champKey, Optional<BatWithId> bat, Hasher hasher) {
        CachingStorage cache = new CachingStorage(this, 100, 100 * 1024);
        return ChampWrapper.create((Cid)root, x -> Futures.of(x.data), cache, hasher, c -> (CborObject.CborMerkleLink) c)
                        .thenCompose(tree -> tree.get(champKey))
                        .thenApply(c -> c.map(x -> x.target).map(MaybeMultihash::of).orElse(MaybeMultihash.empty()))
                        .thenApply(btreeValue -> {
                            if (btreeValue.isPresent())
                                return cache.get((Cid) btreeValue.get(), bat);
                            return Optional.empty();
                        }).thenApply(x -> new ArrayList<>(cache.getCached()));
    }

    @Override
    public CompletableFuture<List<Cid>> put(PublicKeyHash owner,
                                            PublicKeyHash writer,
                                            List<byte[]> signedHashes,
                                            List<byte[]> blocks,
                                            TransactionId tid) {
        return put(writer, blocks, signedHashes, false,Optional.empty());
    }

    @Override
    public CompletableFuture<List<Cid>> putRaw(PublicKeyHash owner,
                                               PublicKeyHash writer,
                                               List<byte[]> signatures,
                                               List<byte[]> blocks,
                                               TransactionId tid,
                                               ProgressConsumer<Long> progressConsumer) {
        return put(writer, blocks, signatures, true, Optional.of(progressConsumer));
    }

    private CompletableFuture<List<Cid>> put(PublicKeyHash writer,
                                             List<byte[]> blocks,
                                             List<byte[]> signatures,
                                             boolean isRaw,
                                             Optional<ProgressConsumer<Long>> progressConsumer) {
        return Futures.combineAllInOrder(IntStream.range(0, blocks.size())
                .mapToObj(i -> hashToCid(blocks.get(i), isRaw)
                        .thenApply(cid -> put(cid, new OpLog.BlockWrite(writer, signatures.get(i), blocks.get(i), isRaw, progressConsumer))))
                .collect(Collectors.toList()));
    }

    private synchronized Cid put(Cid cid, OpLog.BlockWrite block) {
        storage.put(cid, block);
        return cid;
    }

    @Override
    public CompletableFuture<List<FragmentWithHash>> downloadFragments(PublicKeyHash owner,
                                                                       List<Cid> hashes,
                                                                       List<BatWithId> bats,
                                                                       Hasher h,
                                                                       ProgressConsumer<Long> monitor,
                                                                       double spaceIncreaseFactor) {
        return NetworkAccess.downloadFragments(hashes, bats, this, h, monitor, spaceIncreaseFactor);
    }

    @Override
    public synchronized CompletableFuture<Optional<byte[]>> getRaw(Cid hash, Optional<BatWithId> bat) {
        OpLog.BlockWrite local = storage.get(hash);
        if (local != null)
            return Futures.of(Optional.of(local.block));
        return target.getRaw(hash, bat);
    }

    @Override
    public synchronized CompletableFuture<Optional<CborObject>> get(Cid hash, Optional<BatWithId> bat) {
        return getRaw(hash, bat)
                .thenApply(opt -> opt.map(CborObject::fromByteArray));
    }

    @Override
    public CompletableFuture<Cid> put(PublicKeyHash owner,
                                      SigningPrivateKeyAndPublicHash writer,
                                      byte[] block,
                                      Hasher hasher,
                                      TransactionId tid) {
        // Do NOT do signature as this block will likely be GC'd before being committed, so we can delay calculating signatures until commit
        return put(writer.publicKeyHash, Collections.singletonList(block), Collections.singletonList(new byte[0]), false, Optional.empty())
                .thenApply(hashes -> hashes.get(0));
    }

    public CompletableFuture<Boolean> signBlocks(Map<PublicKeyHash, SigningPrivateKeyAndPublicHash> writers) {
        storage.putAll(storage.entrySet().stream()
                .map(e -> {
                    OpLog.BlockWrite block = e.getValue();
                    return new Pair<>(e.getKey(), new OpLog.BlockWrite(block.writer,
                            block.signature.length > 0 ?
                                    block.signature :
                                    writers.get(block.writer).secret.signMessage(e.getKey().getHash()),
                            block.block, block.isRaw, block.progressMonitor));
                })
                .collect(Collectors.toMap(p -> p.left, p -> p.right)));
        return Futures.of(true);
    }

    public synchronized void gc(List<Cid> roots) {
        List<Cid> all = new ArrayList<>(storage.keySet());
        List<Boolean> reachable = new ArrayList<>();
        for (int i=0; i < all.size(); i++)
            reachable.add(false);
        for (Cid root : roots) {
            markReachable(root, reachable, all, storage);
        }
        for (int i=0; i < all.size(); i++) {
            if (! reachable.get(i))
                storage.remove(all.get(i));
        }
    }

    private static void markReachable(Cid current, List<Boolean> reachable, List<Cid> all, Map<Cid, OpLog.BlockWrite> storage) {
        OpLog.BlockWrite block = storage.get(current);
        if (block == null)
            return;
        int index = all.indexOf(current);
        reachable.set(index, true);

        if (current.isRaw())
            return;
        List<Multihash> links = CborObject.fromByteArray(block.block).links();
        for (Multihash link : links) {
            markReachable((Cid)link, reachable, all, storage);
        }
    }

    public synchronized List<Pair<BufferedPointers.WriterUpdate, Optional<CommittedWriterData>>> getAllWriterData(List<BufferedPointers.WriterUpdate> updates) {
        return updates.stream()
                .map(u -> new Pair<>(u, u.currentHash.map(h -> new CommittedWriterData(u.currentHash,
                        WriterData.fromCbor(CborObject.fromByteArray(storage.get(h).block)), u.currentSequence))))
                .collect(Collectors.toList());
    }

    /** Commit the blocks for a given writer
     *
     * @param owner
     * @param writer
     * @param tid
     * @return
     */
    public synchronized CompletableFuture<Boolean> commit(PublicKeyHash owner,
                                                          PublicKeyHash writer,
                                                          TransactionId tid) {
        // write blocks in batches of up to 50 all in 1 transaction
        List<OpLog.BlockWrite> forWriter = new ArrayList<>();
        Set<Cid> toRemove = new HashSet<>();
        for (Map.Entry<Cid, OpLog.BlockWrite> e : storage.entrySet()) {
            if (! Objects.equals(e.getValue().writer, writer))
                continue;
            forWriter.add(e.getValue());
            toRemove.add(e.getKey());
        }
        toRemove.forEach(storage::remove);

        int maxBlocksPerBatch = ContentAddressedStorage.MAX_BLOCK_AUTHS;
        List<List<OpLog.BlockWrite>> cborBatches = new ArrayList<>();
        List<List<OpLog.BlockWrite>> rawBatches = new ArrayList<>();

        int cborCount = 0, rawcount = 0;
        if (! cborBatches.isEmpty() && ! cborBatches.get(cborBatches.size() - 1).isEmpty())
            cborBatches.add(new ArrayList<>());
        if (! rawBatches.isEmpty() && ! rawBatches.get(rawBatches.size() - 1).isEmpty())
            rawBatches.add(new ArrayList<>());
        for (OpLog.BlockWrite val : forWriter) {
            List<List<OpLog.BlockWrite>> batches = val.isRaw ? rawBatches : cborBatches;
            int count = val.isRaw ? rawcount : cborCount;
            if (count % maxBlocksPerBatch == 0)
                batches.add(new ArrayList<>());
            batches.get(batches.size() - 1).add(val);
            count = (count + 1) % maxBlocksPerBatch;
            if (val.isRaw)
                rawcount = count;
            else
                cborCount = count;
        }
        return Futures.combineAllInOrder(rawBatches.stream()
                        .filter(b -> ! b.isEmpty())
                        .map(batch -> target.putRaw(owner, writer,
                                batch.stream().map(w -> w.signature).collect(Collectors.toList()),
                                batch.stream().map(w -> w.block).collect(Collectors.toList()), tid, x-> {})
                                .thenApply(res -> {
                                    batch.stream().forEach(w ->  w.progressMonitor.ifPresent(m -> m.accept((long)w.block.length)));
                                    return res;
                                }))
                        .collect(Collectors.toList()))
                .thenCompose(a -> Futures.combineAllInOrder(cborBatches.stream()
                        .filter(b -> ! b.isEmpty())
                        .map(batch -> target.put(owner, writer,
                                batch.stream().map(w -> w.signature).collect(Collectors.toList()),
                                batch.stream().map(w -> w.block).collect(Collectors.toList()), tid))
                        .collect(Collectors.toList())))
                .thenApply(a -> true);
    }

    public BufferedStorage clone() {
        return new BufferedStorage(target, hasher);
    }

    public BufferedStorage withStorage(Function<ContentAddressedStorage, ContentAddressedStorage> modifiedStorage) {
        return new BufferedStorage(modifiedStorage.apply(target), hasher);
    }

    public synchronized void clear() {
        storage.clear();
    }

    public synchronized int size() {
        return storage.size();
    }

    @Override
    public synchronized CompletableFuture<Optional<Integer>> getSize(Multihash block) {
        if (! storage.containsKey(block))
            return target.getSize(block);
        return CompletableFuture.completedFuture(Optional.of(storage.get(block).block.length));
    }

    public CompletableFuture<Cid> hashToCid(byte[] input, boolean isRaw) {
        return hasher.hash(input, isRaw);
    }

    public int totalSize() {
        return storage.values().stream().mapToInt(a -> a.block.length).sum();
    }
}
