package peergos.shared.storage;

import peergos.shared.io.ipfs.cid.Cid;
import peergos.shared.user.NativeJSCache;
import peergos.shared.util.Futures;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class JSBlockCache implements BlockCache {
    //private final NativeJSCache cache = new NativeJSCache();
    private HashMap<String, byte[]> cache = new HashMap<>();

    public JSBlockCache(int maxSizeMiB) {

        //cache.init(maxSizeMiB);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid hash) {

        byte[] res = cache.get(hash.toString());
        if (res == null) {
            return Futures.of(Optional.empty());
        } else {
            return Futures.of(Optional.of(res));
        }
    }

    @Override
    public boolean hasBlock(Cid hash) {
        return cache.containsKey(hash.toString());
    }

    @Override
    public CompletableFuture<Boolean> put(Cid hash, byte[] data) {

        cache.put(hash.toString(), data);
        return Futures.of(true);
    }

    @Override
    public CompletableFuture<Boolean> clear() {
        cache.clear();
        return Futures.of(true);
    }
}
