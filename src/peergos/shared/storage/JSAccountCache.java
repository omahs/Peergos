package peergos.shared.storage;

import peergos.shared.cbor.CborObject;
import peergos.shared.crypto.asymmetric.PublicSigningKey;
import peergos.shared.io.ipfs.multibase.Multibase;
import peergos.shared.login.LoginCache;
import peergos.shared.user.LoginData;
import peergos.shared.user.NativeJSAccountCache;
import peergos.shared.user.UserStaticData;
import peergos.shared.util.Futures;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class JSAccountCache implements LoginCache {

    //private final NativeJSAccountCache cache = new NativeJSAccountCache();
    private HashMap<String, byte[]> cache = new HashMap<>();

    public JSAccountCache() {

        //cache.init();
    }
    @Override
    public CompletableFuture<Boolean> setLoginData(LoginData login) {
        byte[] entrySerialized = login.entryPoints.toCbor().serialize();
        return setLoginData(login.username, entrySerialized);
    }

    @Override
    public CompletableFuture<UserStaticData> getEntryData(String username, PublicSigningKey authorisedReader) {
        return getEntryDataInternal(username).thenApply(entryPoints -> {
            if (entryPoints == null) {
                throw new RuntimeException("Client Offline!");
            }
            return UserStaticData.fromCbor(CborObject.fromByteArray(entryPoints));
        });
    }

    public CompletableFuture<Boolean> setLoginData(String key, byte[] entryPoints) {
        cache.put(key, entryPoints);
        return Futures.of(true);
    }

    public CompletableFuture<byte[]> getEntryDataInternal(String key) {
        byte[] res = cache.get(key);
        return Futures.of(res);
    }
}
