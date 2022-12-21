package peergos.server.tests;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import peergos.server.Builder;
import peergos.server.Main;
import peergos.server.util.Args;
import peergos.shared.Crypto;
import peergos.shared.NetworkAccess;
import peergos.shared.crypto.symmetric.SymmetricKey;
import peergos.shared.social.FollowRequestWithCipherText;
import peergos.shared.user.UserContext;
import peergos.shared.user.fs.AbsoluteCapability;
import peergos.shared.user.fs.AsyncReader;
import peergos.shared.user.fs.FileWrapper;
import peergos.shared.util.ArrayOps;
import peergos.shared.util.PathUtil;

import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static peergos.server.tests.PeergosNetworkUtils.ensureSignedUp;

@RunWith(Parameterized.class)
public class KevTests {

    private static Args args = UserTests.buildArgs()
            .with("useIPFS", "false")
            .with("default-quota", Long.toString(2 * 1024 * 1024));

    private static int RANDOM_SEED = 666;
    private final NetworkAccess network;
    private static final Crypto crypto = Main.initCrypto();

    private static Random random = new Random(RANDOM_SEED);

    public KevTests(Args args) throws Exception {
        //this.network = Builder.buildJavaNetworkAccess(new URL("http://localhost:" + args.getInt("port")), false).get();
        this.network = peergos.shared.NetworkAccess.buildJS(new URL("http://localhost:" + args.getInt("port")),
                false,0, true).join();
    }

    @Parameterized.Parameters()
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {args}
        });
    }

    @BeforeClass
    public static void init() {
        Main.PKI_INIT.main(args);
    }

    public static String generateUsername(Random random) {
        return "username-" + Math.abs(random.nextInt() % 1_000_000_000);
    }

    public static String generatePassword() {
        return ArrayOps.bytesToHex(crypto.random.randomBytes(32));
    }

    @Test
    public void kevTest()  {
        NetworkAccess sharerNode = network;
        NetworkAccess shareeNode = network;
        int shareeCount = 1;
        Random random = new Random();
        //sign up a user on sharerNode

        String sharerUsername = generateUsername(random);
        String sharerPassword = generatePassword();
        UserContext sharerUser = ensureSignedUp(sharerUsername, sharerPassword, sharerNode.clear(), crypto);

        //sign up some users on shareeNode
        List<String> shareePasswords = IntStream.range(0, shareeCount)
                .mapToObj(i -> generatePassword())
                .collect(Collectors.toList());
        List<UserContext> shareeUsers = getUserContextsForNode(shareeNode, random, shareeCount, shareePasswords);

        // friend sharer with others
        friendBetweenGroups(Arrays.asList(sharerUser), shareeUsers);

        // upload a file to "a"'s space
        FileWrapper u1Root = sharerUser.getUserRoot().join();
        String filename = "somefile.txt";
        byte[] originalFileContents = "Hello!".getBytes();
        AsyncReader resetableFileInputStream = AsyncReader.build(originalFileContents);
        FileWrapper uploaded = u1Root.uploadOrReplaceFile(filename, resetableFileInputStream, originalFileContents.length,
                sharerUser.network, crypto, l -> {}).join();

        // share the file from sharer to each of the sharees
        String filePath = sharerUser.username + "/" + filename;
        FileWrapper u1File = sharerUser.getByPath(filePath).join().get();
        byte[] originalStreamSecret = u1File.getFileProperties().streamSecret.get();
        sharerUser.shareWriteAccessWith(PathUtil.get(sharerUser.username, filename), shareeUsers.stream().map(u -> u.username).collect(Collectors.toSet())).join();

        // check other users can read the file
        for (UserContext userContext : shareeUsers) {
            Optional<FileWrapper> sharedFile = userContext.getByPath(filePath).join();
            Assert.assertTrue("shared file present", sharedFile.isPresent());
            Assert.assertTrue("File is writable", sharedFile.get().isWritable());
            System.out.println("in here");
        }
    }

    public static void friendBetweenGroups(List<UserContext> a, List<UserContext> b) {
        for (UserContext userA : a) {
            for (UserContext userB : b) {
                // send initial request
                userA.sendFollowRequest(userB.username, SymmetricKey.random()).join();

                // make sharer reciprocate all the follow requests
                List<FollowRequestWithCipherText> sharerRequests = userB.processFollowRequests().join();
                for (FollowRequestWithCipherText u1Request : sharerRequests) {
                    AbsoluteCapability pointer = u1Request.req.entry.get().pointer;
                    Assert.assertTrue("Read only capabilities are shared", ! pointer.wBaseKey.isPresent());
                    boolean accept = true;
                    boolean reciprocate = true;
                    userB.sendReplyFollowRequest(u1Request, accept, reciprocate).join();
                }

                // complete the friendship connection
                userA.processFollowRequests().join();
            }
        }
    }

    public static List<UserContext> getUserContextsForNode(NetworkAccess network, Random random, int size, List<String> passwords) {
        return IntStream.range(0, size)
                .mapToObj(e -> {
                    String username = generateUsername(random);
                    String password = passwords.get(e);
                    try {
                        return ensureSignedUp(username, password, network.clear(), crypto);
                    } catch (Exception ioe) {
                        throw new IllegalStateException(ioe);
                    }
                }).collect(Collectors.toList());
    }
}
