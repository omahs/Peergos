package peergos.server;
import java.util.logging.*;
import peergos.server.util.Logging;

import peergos.server.corenode.*;
import peergos.server.mutable.*;
import peergos.server.social.*;
import peergos.server.storage.*;
import peergos.shared.*;
import peergos.shared.crypto.hash.*;
import peergos.shared.mutable.*;
import peergos.shared.storage.*;
import peergos.shared.user.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 *  Use this class to experiment with existing data without committing any changes or writing any data to disk
 */
public class Playground {
    private static final Logger LOG = Logging.LOG();

    public static void main(String[] args) throws Exception {
        Crypto crypto = Main.initCrypto();
        NetworkAccess source = NetworkAccess.buildJava(new URL("https://demo.peergos.net")).get();
        NetworkAccess nonWriteThrough = NonWriteThroughNetwork.build(source);

        String username = args[0];
        Console console = System.console();
        String password = new String(console.readPassword("Enter password for " + username + ":"));
        UserContext context = UserContext.signIn(username, password, nonWriteThrough, crypto).get();

        // invert the following two lines to actually commit the experiment
        experiment(username, context, nonWriteThrough);
//        experiment(username, context, source);

        // Can we still log in?
        UserContext context2 = UserContext.signIn(username, password, nonWriteThrough, crypto).get();
        LOG.info(context2.getUserRoot().get().getName());
    }

    private static void experiment(String username,
                                   UserContext context,
                                   NetworkAccess network) throws Exception {
        // Do something dangerous (you only live once)
        Set<PublicKeyHash> ownedKeys = WriterData.getOwnedKeysRecursive(username, network.coreNode,
                network.mutable, network.dhtClient, network.hasher).join();
        for (PublicKeyHash ownedKey : ownedKeys) {
            if (ownedKey.equals(context.signer.publicKeyHash))
                continue; // only the writer has a tree
            CommittedWriterData existing = WriterData.getWriterData(context.signer.publicKeyHash, ownedKey,
                    network.mutable, network.dhtClient).get();
            if (existing.props.tree.isPresent())
                continue;
            // Do something risky
        }
    }
}
