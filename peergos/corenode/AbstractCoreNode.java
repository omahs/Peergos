package peergos.corenode;

import peergos.crypto.*;
import peergos.user.fs.Fragment;
import peergos.util.ArrayOps;
import peergos.util.ByteArrayWrapper;
import peergos.util.Serialize;

import java.sql.*;
import java.util.*;
import java.net.*;
import java.io.*;

import static peergos.crypto.UserPublicKey.HASH_BYTES;

public abstract class AbstractCoreNode
{
    public static final int PORT = 9999;
    public static final float FRAC_TOLERANCE = 0.001f;
    private static final long DEFAULT_FRAGMENT_LENGTH = Fragment.SIZE;

    private static AbstractCoreNode memory;
    public synchronized static AbstractCoreNode getMemoryCoreNode() {
        if (memory != null)
            try {
                memory = SQLiteCoreNode.build(":memory:");
            } catch (SQLException s) {
                s.printStackTrace();
            }
        return memory;
    }

    /**
     * Maintains all network metadata in encrypted form, without exposing friendship network.
     */ 

    public static class MetadataBlob
    {
        static final MetadataBlob EMPTY = new MetadataBlob(new ByteArrayWrapper(new byte[0]), new byte[0]);

        ByteArrayWrapper metadata;
        byte[] fragmentHashes;

        public MetadataBlob(ByteArrayWrapper metadata, byte[] fragmentHashes)
        {
            this.metadata = metadata;// 32 bytes AES key + 16 bytes IV + Y bytes (pointer to next blob) + 256 bytes filename + Z bytes pointer to parent
            this.fragmentHashes = fragmentHashes;
        }

        public boolean containsHash(byte[] hash)
        {
            if (fragmentHashes == null)
                return false;

            for (int pos=0; pos + HASH_BYTES <= fragmentHashes.length; pos += HASH_BYTES)
                if (Arrays.equals(hash, Arrays.copyOfRange(fragmentHashes, pos, pos + HASH_BYTES)))
                    return true;

            return false;
        }
        public ByteArrayWrapper metadata(){return metadata;}
        public byte[] fragmentHashes(){return fragmentHashes;}
    }

    static class UserData
    {
        public static final int MAX_PENDING_FOLLOWERS = 100;

        private final Set<ByteArrayWrapper> followRequests;
        private final Set<UserPublicKey> followers;
        private final Map<UserPublicKey, Map<ByteArrayWrapper, MetadataBlob> > metadata;

        private ByteArrayWrapper staticData;

        UserData(ByteArrayWrapper clearanceData)
        {
            this.staticData = clearanceData;
            this.followRequests = new HashSet<ByteArrayWrapper>();
            this.followers = new HashSet<UserPublicKey>();
            this.metadata = new HashMap<UserPublicKey, Map<ByteArrayWrapper, MetadataBlob> >();
        }
    }  

    static class StorageNodeState
    {
        private final Set<ByteArrayWrapper> fragmentHashesOnThisNode;
        private final UserPublicKey owner;
        private final Map<UserPublicKey, Float> storageFraction;
        private final InetSocketAddress address;
        StorageNodeState(UserPublicKey owner, InetSocketAddress address, Map<UserPublicKey, Float> fractions)
        {
            this.owner = owner;
            this.address = address;
            this.storageFraction = new HashMap<UserPublicKey,Float>(fractions);
            this.fragmentHashesOnThisNode = new HashSet();
        }

        public InetSocketAddress address(){return address;}
        public Map<UserPublicKey,Float> fractions()
        {
            return new HashMap<UserPublicKey, Float>(storageFraction);
        }
        public long getSize()
        {
            return calculateSize();
        }

        private long calculateSize()
        {
            return fragmentLength()*fragmentHashesOnThisNode.size();
        }

        public boolean addHash(byte[] hash)
        {
            return fragmentHashesOnThisNode.add(new ByteArrayWrapper(hash));
        }

        public int hashCode(){return address.hashCode();}
        public boolean equals(Object that)
        {
            if (! (that instanceof StorageNodeState))
                return false;

            return ((StorageNodeState) that).address.equals(this.address);
        }
    } 

    private final Map<UserPublicKey, UserData> userMap;
    private final Map<String, UserPublicKey> userNameToPublicKeyMap;
    private final Map<UserPublicKey, String> userPublicKeyToNameMap;

    //
    // quota stuff
    //

    //
    // aims
    // 1. calculate how much storage space a user has donated to other users (and themselves)  (via 1 or more storage nodes) 
    // 2. calculate how much storage space a Storage node has available to which other users
    //
    private final Map<InetSocketAddress, StorageNodeState> storageStates;
    private final Map<UserPublicKey, Set<StorageNodeState> > userStorageFactories;

    public AbstractCoreNode()
    {
        this.userMap = new HashMap<UserPublicKey, UserData>();
        this.userNameToPublicKeyMap = new HashMap<String, UserPublicKey>();
        this.userPublicKeyToNameMap = new HashMap<UserPublicKey, String>();

        this.storageStates = new HashMap<InetSocketAddress, StorageNodeState>();
        this.userStorageFactories =new HashMap<UserPublicKey, Set<StorageNodeState> > ();
    }

    public static long fragmentLength(){return DEFAULT_FRAGMENT_LENGTH;}

    public synchronized UserPublicKey getPublicKey(String username)
    {
        return userNameToPublicKeyMap.get(username);
    }

    public synchronized String getUsername(byte[] encodedUserKey)
    {
        UserPublicKey key = new UserPublicKey(encodedUserKey);
        String name = userPublicKeyToNameMap.get(key);
        if (name == null)
            name = "";
        return name;
    }

    /*
     * @param userKey X509 encoded public key
     * @param signature of bytes in the username, with the user private key
     * @param username the username that is being claimed
     */
    public boolean addUsername(String username, byte[] encodedUserKey, byte[] signed, byte[] staticData)
    {
        UserPublicKey key = new UserPublicKey(encodedUserKey);

        if (! key.isValidSignature(signed, ArrayOps.concat(username.getBytes(), encodedUserKey, staticData)))
            return false;

        return addUsername(username, key, new ByteArrayWrapper(staticData));
    }

    protected synchronized boolean addUsername(String username, UserPublicKey key, ByteArrayWrapper clearanceData)
    {
        if (userNameToPublicKeyMap.containsKey(username))
            return false;
        if (userPublicKeyToNameMap.containsKey(key))
            return false;

        userNameToPublicKeyMap.put(username, key); 
        userPublicKeyToNameMap.put(key, username); 
        userMap.put(key, new UserData(clearanceData));
        userStorageFactories.put(key, new HashSet<>());
        return true;
    }

    public boolean updateStaticData(UserPublicKey owner, byte[] signedStaticData)
    {
        return updateClearanceData(owner, new ByteArrayWrapper(owner.unsignMessage(signedStaticData)));
    }

    public synchronized byte[] getStaticData(UserPublicKey owner)
    {
        UserData userData = userMap.get(owner);
        return userData != null ? Arrays.copyOf(userData.staticData.data, userData.staticData.data.length) : new byte[0];
    }

    protected synchronized boolean updateClearanceData(UserPublicKey owner, ByteArrayWrapper clearanceData)
    {
        UserData userData = userMap.get(owner);
        if (userData == null)
            return false;
        userData.staticData = clearanceData;
        return true;
    }
    public synchronized boolean followRequest(UserPublicKey target, byte[] encryptedPermission)
    {
        UserData userData = userMap.get(target);
        if (userData == null)
            return false;
        if (userData.followRequests.size() > UserData.MAX_PENDING_FOLLOWERS)
            return false;
        userData.followRequests.add(new ByteArrayWrapper(encryptedPermission));
        return true;
    }

    public synchronized byte[] getFollowRequests(UserPublicKey owner)
    {
        UserData userData = userMap.get(owner);
        if (userData == null)
            return new byte[4];

        ByteArrayOutputStream bout  =new ByteArrayOutputStream();
        DataOutput dout = new DataOutputStream(bout);
        try {
            dout.writeInt(userData.followRequests.size());
            for (ByteArrayWrapper req : userData.followRequests)
                Serialize.serialize(req.data, dout);
            return bout.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean removeFollowRequest(UserPublicKey owner, byte[] data, byte[] signedHash)
    {
        UserPublicKey us = null;
        ByteArrayWrapper baw = new ByteArrayWrapper(data);
        synchronized(this)

        {
            us = userNameToPublicKeyMap.get(owner);
            if (us == null || ! userMap.get(owner).followRequests.contains(baw))
                return false; 
        }
        if (! us.isValidSignature(signedHash, UserPublicKey.hash(data)))
            return false;

        return removeFollowRequest(us, new ByteArrayWrapper(data));
    }

    protected synchronized boolean removeFollowRequest(UserPublicKey target, ByteArrayWrapper baw)
    {
        UserData userData = userMap.get(target);
        if (userData == null)
            return false;
        return userData.followRequests.remove(baw);
    }

    /*
     * @param userKey X509 encoded key of user that wishes to add a friend
     * @param signedHash the SHA hash of userBencodedKey, signed with the user private key 
     * @param encodedFriendName the bytes of the friendname sined with userKey 
     */
    public boolean allowSharingKey(UserPublicKey owner, byte[] signedSharingPublicKey)
    {
        byte[] encodedSharingPublicKey = Arrays.copyOfRange(signedSharingPublicKey, TweetNaCl.SIGNATURE_SIZE_BYTES, signedSharingPublicKey.length);

        if (owner == null || ! owner.isValidSignature(signedSharingPublicKey, encodedSharingPublicKey))
            return false;

        return allowSharingKey(owner, new UserPublicKey(encodedSharingPublicKey));
    }

    protected synchronized boolean allowSharingKey(UserPublicKey owner, UserPublicKey sharingPublicKey)
    {
        UserData userData = userMap.get(owner);
        if (userData == null)
            return false;
        if (userData.followers.contains(sharingPublicKey))
            return false;

        userData.followers.add(sharingPublicKey);
        if (! userData.metadata.containsKey(sharingPublicKey))
            userData.metadata.put(sharingPublicKey, new HashMap<ByteArrayWrapper, MetadataBlob>());
        return true;
    }

    public boolean banSharingKey(UserPublicKey owner, byte[] encodedsharingPublicKey, byte[] signedHash)
    {
        if (! owner.isValidSignature(signedHash, UserPublicKey.hash(encodedsharingPublicKey)))
            return false;

        UserPublicKey sharingPublicKey = new UserPublicKey(encodedsharingPublicKey);
        return banSharingKey(owner, sharingPublicKey);
    }

    protected synchronized boolean banSharingKey(UserPublicKey owner, UserPublicKey sharingPublicKey)
    {
        UserData userData = userMap.get(owner);
        if (userData == null)
            return false; 
        return userData.followers.remove(sharingPublicKey);
    }

    /*
     * @param userKey X509 encoded key of user that wishes to add a fragment
     * @param signedHash the SHA hash of encodedFragmentData, signed with the user private key 
     * @param encodedFragmentData fragment meta-data encoded with userKey
     */ 

    //public boolean addMetadataBlob(byte[] userKey, byte[] signedHash, byte[] encodedFragmentData)
    public boolean addMetadataBlob(UserPublicKey owner, byte[] encodedSharingPublicKey, byte[] mapKey, byte[] metadataBlob, byte[] sharingKeySignedHash)
    {
        UserPublicKey sharingKey = new UserPublicKey(encodedSharingPublicKey);
        System.out.printf("Adding metadata blob at owner:%s writer:%s mapKey:%s\n",
                ArrayOps.bytesToHex(owner.getPublicKeys()), ArrayOps.bytesToHex(encodedSharingPublicKey), ArrayOps.bytesToHex(mapKey));
        synchronized(this)
        {
            if (!userMap.get(owner).followers.contains(sharingKey))
                return false;
        }

        if (!sharingKey.isValidSignature(sharingKeySignedHash, ArrayOps.concat(mapKey, metadataBlob)))
            return false;

        return addMetadataBlob(owner, sharingKey, mapKey, metadataBlob);
    }

    protected synchronized boolean addMetadataBlob(UserPublicKey owner, UserPublicKey sharingKey, byte[] mapKey, byte[] metadataBlob)
    {
        UserData userData = userMap.get(owner);

        if (userData == null)
            return false;

        Map<ByteArrayWrapper, MetadataBlob> fragments = userData.metadata.get(sharingKey);
        if (fragments == null)
            return false;

        ByteArrayWrapper keyW = new ByteArrayWrapper(mapKey);
        if (fragments.containsKey(keyW)) {
            // for now just overwrite it. This is vulnerable to replay attacks so eventually we'll need to sign the
            // previous value as well, or some kind of version cas
            // TODO return false;
        }

        fragments.put(keyW, new MetadataBlob(new ByteArrayWrapper(metadataBlob), null));
        return true;
    }

    public boolean addFragmentHashes(UserPublicKey owner, byte[] encodedSharingPublicKey, byte[] mapKey, byte[] metadataBlob, List<ByteArrayWrapper> fragmentHashes, byte[] sharingKeySignedHash)
    {
        UserPublicKey sharingKey = new UserPublicKey(encodedSharingPublicKey);
        synchronized(this)
        {
            if (!userMap.get(owner).followers.contains(sharingKey))
                return false;
        }
        if (remainingStorage(owner) < fragmentLength())
            return false;

        byte[] allHashes = ArrayOps.concat(fragmentHashes);
        if (! sharingKey.isValidSignature(sharingKeySignedHash, ArrayOps.concat(mapKey, metadataBlob, allHashes)))
            return false;

        return addFragmentHashes(owner, sharingKey, mapKey, allHashes);
    }

    protected synchronized boolean addFragmentHashes(UserPublicKey owner, UserPublicKey sharingKey, byte[] mapKey, byte[] allHashes)
    {
        if (!userMap.get(owner).followers.contains(sharingKey))
            return false;
        if (remainingStorage(owner) < fragmentLength())
            return false;
        UserData userData = userMap.get(owner);
        if (userData == null)
            return false;

        Map<ByteArrayWrapper, MetadataBlob> fragments = userData.metadata.get(sharingKey);
        if (fragments == null)
            return false;

        MetadataBlob meta = fragments.get(new ByteArrayWrapper(mapKey));
        if (meta == null)
            return false;

        // add hashes
        meta.fragmentHashes = allHashes;
        return true;
    }

    public synchronized byte[] getFragmentHashes(UserPublicKey owner, UserPublicKey sharingKey, byte[] mapKey) {
        if (!userMap.get(owner).followers.contains(sharingKey))
            return null;
        UserData userData = userMap.get(owner);
        if (userData == null)
            return null;
        Map<ByteArrayWrapper, MetadataBlob> fragments = userData.metadata.get(sharingKey);
        if (fragments == null)
            return null;

        MetadataBlob meta = fragments.get(new ByteArrayWrapper(mapKey));
        if (meta == null)
            return null;

        return meta.fragmentHashes;
    }


    // should delete fragments from dht as well (once that call exists)
    public boolean removeMetadataBlob(UserPublicKey owner, byte[] encodedSharingKey, byte[] mapKey, byte[] sharingKeySignedMapKey)
    {
        UserPublicKey sharingKey = new UserPublicKey(encodedSharingKey);

        synchronized(this)
        {
            if (! userMap.get(owner).followers.contains(sharingKey))
                return false;
        }

        if (! sharingKey.isValidSignature(sharingKeySignedMapKey, mapKey))
            return false;

        return removeMetadataBlob(owner, sharingKey, mapKey);
    }

    protected synchronized boolean removeMetadataBlob(UserPublicKey owner, UserPublicKey sharingKey, byte[] mapKey)
    {
        UserData userData = userMap.get(owner);

        if (userData == null)
            return false;
        Map<ByteArrayWrapper, MetadataBlob> fragments = userData.metadata.get(sharingKey);
        if (fragments == null)
            return false;

        System.out.printf("Removing metadata blob at owner:%s writer:%s mapKey:%s\n",
                ArrayOps.bytesToHex(owner.getPublicKeys()), ArrayOps.bytesToHex(sharingKey.getPublicKeys()), ArrayOps.bytesToHex(mapKey));
        return fragments.remove(new ByteArrayWrapper(mapKey)) != null;
    }

    /*
     * @param userKey X509 encoded key of user to be removed 
     * @param username to be removed 
     * @param signedHash the SHA hash of the bytes that make up username, signed with the user private key 
     *
     */

    public boolean removeUsername(String username, byte[] userKey, byte[] signedHash)
    {
        UserPublicKey key = new UserPublicKey(userKey);
        String current = userPublicKeyToNameMap.get(key);
        if (current == null || !current.equals(username))
            return false;

        if (! key.isValidSignature(signedHash, UserPublicKey.hash(username)))
            return false;

        return removeUsername(username, key);
    }

    protected synchronized boolean removeUsername(String username, UserPublicKey key)
    {
        userPublicKeyToNameMap.remove(key);
        userMap.remove(key);
        return userNameToPublicKeyMap.remove(username) != null;
    }

    /*
     * @param userKey X509 encoded key of user that wishes to share a fragment 
     * @param signedHash the SHA hash of userKey, signed with the user private key 
     */
    public synchronized Iterator<UserPublicKey> getSharingKeys(UserPublicKey owner)
    {
        UserData userData = userMap.get(owner);
        return Collections.unmodifiableCollection(userData.metadata.keySet()).iterator();

    } 

    public synchronized MetadataBlob getMetadataBlob(String username, byte[] encodedSharingKey, byte[] mapkey) {
        UserPublicKey userKey = userNameToPublicKeyMap.get(username);
        if (userKey == null)
            return null;
        return getMetadataBlob(userKey, encodedSharingKey, mapkey);
    }

    public synchronized MetadataBlob getMetadataBlob(UserPublicKey owner, byte[] encodedSharingKey, byte[] mapKey) {
        UserData userData = userMap.get(owner);
        if (userData == null) {
            System.out.printf("Returning EMPTY metadata blob from owner:%s writer:%s mapKey:%s\n",
                ArrayOps.bytesToHex(owner.getPublicKeys()), ArrayOps.bytesToHex(encodedSharingKey), ArrayOps.bytesToHex(mapKey));
            return MetadataBlob.EMPTY;
        }
        Map<ByteArrayWrapper, MetadataBlob> sharedFragments = userData.metadata.get(new UserPublicKey(encodedSharingKey));
        System.out.printf("Getting metadata blob at owner:%s writer:%s mapKey:%s\n",
                ArrayOps.bytesToHex(owner.getPublicKeys()), ArrayOps.bytesToHex(encodedSharingKey), ArrayOps.bytesToHex(mapKey));
        ByteArrayWrapper key = new ByteArrayWrapper(mapKey);
        if ((sharedFragments == null) || (!sharedFragments.containsKey(key)))
            return null;
        return sharedFragments.get(key);
    }

    protected boolean addStorageNodeState(UserPublicKey owner, InetSocketAddress address)
    {
        Map<UserPublicKey, Float> fracs = new HashMap<>();
        fracs.put(owner, 1.f);
        return addStorageNodeState(owner, address, fracs);
    }
    protected boolean addStorageNodeState(UserPublicKey owner, InetSocketAddress address, Map<UserPublicKey, Float> fracs)
    {
        //
        // validate map entries
        //
        float totalFraction = 0.f;
        for (Float frac: fracs.values())
            if (frac < 0)
                return false;
            else
                totalFraction += frac;

        if (totalFraction -1 > FRAC_TOLERANCE)
            return false;

        StorageNodeState state = new StorageNodeState(owner, address, fracs);
        return addStorageNodeState(state);
    }

    protected synchronized boolean addStorageNodeState(StorageNodeState state)
    {

        if (! userPublicKeyToNameMap.containsKey(state.owner))
            return false;
        if (storageStates.containsKey(state.address))
            return false;

        for (UserPublicKey user: state.storageFraction.keySet())
            if (! userPublicKeyToNameMap.containsKey(user))
                return false;

        storageStates.put(state.address, state);

        for (UserPublicKey user: state.storageFraction.keySet())
        {
            if (userStorageFactories.get(user) == null)
                userStorageFactories.put(user, new HashSet<StorageNodeState>());

            userStorageFactories.get(user).add(state);
        }
        return true;
    }

    public synchronized boolean isFragmentAllowed(UserPublicKey owner, byte[] encodedSharingKey, byte[] mapkey, byte[] hash)
    {
        UserData userData = userMap.get(owner);
        UserPublicKey sharingPublicKey = new UserPublicKey(encodedSharingKey);
        if (userData == null || ! userData.followers.contains(sharingPublicKey))
            return false;

        MetadataBlob blob = userData.metadata.get(sharingPublicKey).get(new ByteArrayWrapper(mapkey));
        if (blob == null)
            return false;

        // The hashes are now buried in the FileRetriever...
        return true;
//        return blob.containsHash(hash);
    }

    public boolean registerFragmentStorage(UserPublicKey spaceDonor, InetSocketAddress node, UserPublicKey owner, byte[] signedKeyPlusHash)
    {
        byte[] encodedSharingKey = Arrays.copyOfRange(signedKeyPlusHash, TweetNaCl.SIGNATURE_SIZE_BYTES, TweetNaCl.SIGNATURE_SIZE_BYTES + UserPublicKey.SIZE);
        byte[] hash = Arrays.copyOfRange(signedKeyPlusHash, TweetNaCl.SIGNATURE_SIZE_BYTES + UserPublicKey.SIZE, signedKeyPlusHash.length);
        UserPublicKey sharingPublicKey = new UserPublicKey(encodedSharingKey);
        if (!sharingPublicKey.isValidSignature(signedKeyPlusHash, Arrays.copyOfRange(signedKeyPlusHash, TweetNaCl.SIGNATURE_SIZE_BYTES, signedKeyPlusHash.length)))
            return false;

        return registerFragmentStorage(spaceDonor, node, owner, sharingPublicKey, hash);
    }

    protected synchronized boolean registerFragmentStorage(UserPublicKey spaceDonor, InetSocketAddress node, UserPublicKey owner, UserPublicKey sharingKey, byte[] hash)
    {
        if (!userStorageFactories.containsKey(spaceDonor))
            return false;

        UserData userData = userMap.get(owner);
        if (userData == null)
            return false;
        if (!userData.followers.contains(sharingKey))
            return false;

        if (!storageStates.containsKey(node))
            addStorageNodeState(spaceDonor, node);
        StorageNodeState donor = storageStates.get(node);
        return donor.addHash(hash);

    }

    public synchronized long getQuota(UserPublicKey user)
    {
        if (! userPublicKeyToNameMap.containsKey(user))
            return -1l;

        Set<StorageNodeState> storageStates = userStorageFactories.get(user);
        if (storageStates == null)
            return 0l;
        long quota = 0l;

        for (StorageNodeState state: storageStates)
            quota += state.getSize()* state.storageFraction.get(user);

        return quota;    
    }

    public synchronized long getUsage(UserPublicKey owner)
    {
        long usage = 0l;
        for (Map<ByteArrayWrapper, MetadataBlob> fragmentsMap: userMap.get(owner).metadata.values())
            for (MetadataBlob blob: fragmentsMap.values())
                if (blob.fragmentHashes != null)
                    usage += blob.fragmentHashes.length/UserPublicKey.HASH_BYTES * fragmentLength();

        return usage;
    }



    private synchronized long remainingStorage(UserPublicKey user)
    {
        long quota = getQuota(user);
        long usage = getUsage(user);

        return Math.max(0, quota - usage);
    }

    public abstract void close() throws IOException;

    public static AbstractCoreNode getDefault()
    {
        return new AbstractCoreNode() {
            @Override
            public void close() throws IOException {

            }
        };
    }
}