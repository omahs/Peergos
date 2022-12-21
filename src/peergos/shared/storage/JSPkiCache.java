package peergos.shared.storage;

import peergos.shared.cbor.CborObject;
import peergos.shared.corenode.PkiCache;
import peergos.shared.corenode.UserPublicKeyLink;
import peergos.shared.crypto.hash.PublicKeyHash;
import peergos.shared.io.ipfs.multibase.Multibase;
import peergos.shared.user.NativeJSPkiCache;
import peergos.shared.util.Futures;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class JSPkiCache implements PkiCache {

    //private final NativeJSPkiCache cache = new NativeJSPkiCache();
    private HashMap<String, List<String>> cache = new HashMap<>();
    private HashMap<String, String> pkiOwnerToUsername = new HashMap<>();

    public JSPkiCache() {

        //cache.init();
    }

    @Override
    public CompletableFuture<List<UserPublicKeyLink>> getChain(String username) {
        return getChainInternal(username).thenApply(serialisedUserPublicKeyLinks -> {
            if (serialisedUserPublicKeyLinks.isEmpty())
                throw new RuntimeException("Client Offline!");
            List<UserPublicKeyLink> list = new ArrayList();
            for(String userPublicKeyLink :  serialisedUserPublicKeyLinks) {
                list.add(UserPublicKeyLink.fromCbor(CborObject.fromByteArray(Multibase.decode(userPublicKeyLink))));
            }
            return list;
        });
    }

    @Override
    public CompletableFuture<Boolean> setChain(String username, List<UserPublicKeyLink> chain) {
        String[] serialisedUserPublicKeyLinks = new String[chain.size()];
        for(int i =0; i < chain.size(); i++) {
            serialisedUserPublicKeyLinks[i] = Multibase.encode(Multibase.Base.Base58BTC, chain.get(i).serialize());
        }
        PublicKeyHash owner = chain.get(chain.size() - 1).owner;
        String serialisedOwner = new String(Base64.getEncoder().encode(owner.serialize()));
        return setChainInternal(username, serialisedUserPublicKeyLinks, serialisedOwner);
    }

    @Override
    public CompletableFuture<String> getUsername(PublicKeyHash key) {
        return getUsername(new String(Base64.getEncoder().encode(key.serialize()))).thenApply(username -> {
           if (username.isEmpty()) {
               throw new RuntimeException("Client Offline!");
           }
           return username;
        });
    }

    public CompletableFuture<Boolean> setChainInternal(String username, String[] serialisedUserPublicKeyLinkChain, String serialisedOwner)
    {
        cache.put(username, List.of(serialisedUserPublicKeyLinkChain));
        pkiOwnerToUsername.put(serialisedOwner, username);
        return Futures.of(true);
    }
    public CompletableFuture<List<String>> getChainInternal(String username)
    {
        List<String> res = cache.get(username);
        if (res == null) {
            return Futures.of(Collections.emptyList());
        } else {
            return Futures.of(res);
        }
    }
    public CompletableFuture<String> getUsername(String serialisedPublicKeyHash) {
        String res = pkiOwnerToUsername.get(serialisedPublicKeyHash);
        if (res == null) {
            return Futures.of("");
        } else {
            return Futures.of(res);
        }
    }
}
