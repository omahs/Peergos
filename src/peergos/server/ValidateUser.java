package peergos.server;

import peergos.server.util.*;
import peergos.shared.*;
import peergos.shared.cbor.*;
import peergos.shared.crypto.hash.*;
import peergos.shared.io.ipfs.cid.*;
import peergos.shared.io.ipfs.multihash.*;
import peergos.shared.user.*;

import java.net.*;
import java.util.*;
import java.util.logging.*;

/** This utility check that all of a users mutable pointers and blocks are present as far as the network can tell
 *
 */
public class ValidateUser {

    public static void main(String[] args) throws Exception {
        Crypto crypto = Main.initCrypto();
        NetworkAccess network = Builder.buildJavaNetworkAccess(new URL("https://peergos.net"), true).get();
        String username = args[0];
        Optional<PublicKeyHash> identity = network.coreNode.getPublicKeyHash(username).join();
        Set<PublicKeyHash> ownedKeys = WriterData.getOwnedKeysRecursive(username, network.coreNode, network.mutable,
                network.dhtClient, network.hasher).join();
        for (PublicKeyHash ownedKey : ownedKeys) {
            validateWriter(identity.get(), ownedKey, network);
        }
    }

    private static void validateWriter(PublicKeyHash owner, PublicKeyHash writer, NetworkAccess network) {
        MaybeMultihash target = network.mutable.getPointerTarget(owner, writer, network.dhtClient).join().updated;
        if (! target.isPresent()) {
            Logging.LOG().log(Level.WARNING, "Skipping unretrievable mutable pointer for: " + writer);
            return;
        }

        validateBlock(target.get(), network);
    }

    private static void validateBlock(Multihash target, NetworkAccess network) {
        Optional<CborObject> block = network.dhtClient.get((Cid)target, Optional.empty()).join();
        if (! block.isPresent())
            throw new IllegalStateException("Couldn't retrieve " + target);

        List<Multihash> links = block.get().links();
        for (Multihash link : links) {
            if (link instanceof Cid && ((Cid) link).codec == Cid.Codec.Raw)
                network.dhtClient.getSize(link).join();
            else
                validateBlock(link, network);
        }
    }
}
