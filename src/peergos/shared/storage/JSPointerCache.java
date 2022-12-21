package peergos.shared.storage;

import peergos.shared.crypto.hash.PublicKeyHash;
import peergos.shared.mutable.PointerCache;
import peergos.shared.user.NativeJSPointerCache;
import peergos.shared.util.Futures;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class JSPointerCache implements PointerCache {
    //private final NativeJSPointerCache cache = new NativeJSPointerCache();
    private HashMap<String, byte[]> cache = new HashMap<>();

    private final ContentAddressedStorage storage;

    public JSPointerCache(int maxItems, ContentAddressedStorage storage) {
        //cache.init(maxItems);
        this.storage = storage;
    }

    @Override
    public CompletableFuture<Boolean> put(PublicKeyHash owner, PublicKeyHash writer, byte[] signedUpdate) {
        return getInternal(owner, writer)
                .thenCompose(current -> storage.getSigningKey(writer).thenCompose(signerOpt -> {
                    if (signerOpt.isEmpty())
                        throw new IllegalStateException("Couldn't retrieve signing key!");
                    if (doUpdate(current, signedUpdate, signerOpt.get())) {
                        String key = owner.toString() + "-" + writer.toString();
                        cache.put(key, signedUpdate);
                        return Futures.of(true);
                    }
                    return Futures.of(false);
                }));
    }
    @Override
    public CompletableFuture<Optional<byte[]>> get(PublicKeyHash owner, PublicKeyHash writer) {
        return getInternal(owner, writer);
    }

    public CompletableFuture<Optional<byte[]>> getInternal(PublicKeyHash owner, PublicKeyHash writer) {
        String key = owner.toString() + "-" + writer.toString();
        byte[] res = cache.get(key);
        if (res == null) {
            return Futures.of(Optional.empty());
        } else {
            return Futures.of(Optional.of(res));
        }
    }
}
