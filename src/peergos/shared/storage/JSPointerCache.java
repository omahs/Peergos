package peergos.shared.storage;

import peergos.shared.crypto.hash.PublicKeyHash;
import peergos.shared.mutable.PointerCache;
import peergos.shared.user.NativeJSPointerCache;
import peergos.shared.util.Futures;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class JSPointerCache implements PointerCache {
    private final NativeJSPointerCache cache = new NativeJSPointerCache();
    private final ContentAddressedStorage storage;

    public JSPointerCache(int maxItems, ContentAddressedStorage storage) {
        cache.init(maxItems);
        this.storage = storage;
    }

    @Override
    public CompletableFuture<Boolean> put(PublicKeyHash owner, PublicKeyHash writer, byte[] signedUpdate) {
        return cache.get(owner, writer)
                .thenCompose(current -> storage.getSigningKey(writer).thenCompose(signerOpt -> {
                    if (signerOpt.isEmpty())
                        throw new IllegalStateException("Couldn't retrieve signing key!");
                    if (doUpdate(current, signedUpdate, signerOpt.get()))
                        return cache.put(owner, writer, signedUpdate);
                    return Futures.of(false);
                }));
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(PublicKeyHash owner, PublicKeyHash writer) {
        return cache.get(owner, writer);
    }
}
