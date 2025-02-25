package peergos.server.storage;

import peergos.server.util.Logging;
import peergos.shared.io.ipfs.cid.*;
import peergos.shared.io.ipfs.multihash.*;
import peergos.shared.storage.*;
import peergos.shared.util.*;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.logging.*;

/** A local file based block cache LRU
 *
 */
public class FileBlockCache implements BlockCache {
    private static final Logger LOG = Logging.LOG();
    private static final int DIRECTORY_DEPTH = 5;
    private final Path root;
    private final long maxSizeBytes;
    private long lastSizeCheckTime = 0;
    private AtomicLong totalSize = new AtomicLong(0);

    public FileBlockCache(Path root, long maxSizeBytes) {
        this.root = root;
        this.maxSizeBytes = maxSizeBytes;
        File rootDir = root.toFile();
        if (!rootDir.exists()) {
            final boolean mkdirs = root.toFile().mkdirs();
            if (!mkdirs)
                throw new IllegalStateException("Unable to create directory " + root);
        }
        if (!rootDir.isDirectory())
            throw new IllegalStateException("File store path must be a directory! " + root);
        applyToAll(c -> getSize(c).join().map(s -> totalSize.addAndGet(s)));
    }

    private Path getFilePath(Cid h) {
        String name = h.toString();

        int depth = DIRECTORY_DEPTH;
        Path path = PathUtil.get("");
        for (int i=0; i < depth; i++)
            path = path.resolve(Character.toString(name.charAt(i)));
        // include full name in filename
        path = path.resolve(name);
        return path;
    }

    /**
     * Remove all files stored as part of this FileContentAddressedStorage.
     */
    public void remove() {
        root.toFile().delete();
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid hash) {
        try {
            if (hash.isIdentity())
                return Futures.of(Optional.of(hash.getHash()));
            Path path = getFilePath(hash);
            File file = root.resolve(path).toFile();
            if (! file.exists()){
                return CompletableFuture.completedFuture(Optional.empty());
            }
            try (DataInputStream din = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                byte[] block = Serialize.readFully(din);
                return CompletableFuture.completedFuture(Optional.of(block));
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasBlock(Cid hash) {
        Path path = getFilePath(hash);
        File file = root.resolve(path).toFile();
        return file.exists();
    }

    public CompletableFuture<Boolean> put(Cid hash, byte[] data) {
        try {
            Path filePath = getFilePath(hash);
            Path target = root.resolve(filePath);
            Path parent = target.getParent();
            File parentDir = parent.toFile();

            if (! parentDir.exists())
                Files.createDirectories(parent);

            for (Path someParent = parent; !someParent.equals(root); someParent = someParent.getParent()) {
                File someParentFile = someParent.toFile();
                if (! someParentFile.canWrite()) {
                    final boolean b = someParentFile.setWritable(true, false);
                    if (!b)
                        throw new IllegalStateException("Could not make " + someParent.toString() + ", ancestor of " + parentDir.toString() + " writable");
                }
            }
            Files.write(target, data, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            totalSize.addAndGet(data.length);
            if (lastSizeCheckTime < System.currentTimeMillis() - 600_000) {
                lastSizeCheckTime = System.currentTimeMillis();
                ForkJoinPool.commonPool().submit(() -> ensureWithinSizeLimit(maxSizeBytes));
            }
            return Futures.of(true);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected List<Cid> getFiles() {
        List<Cid> existing = new ArrayList<>();
        getFilesRecursive(root, existing::add);
        return existing;
    }

    public CompletableFuture<Optional<Integer>> getSize(Multihash h) {
        Path path = getFilePath((Cid)h);
        File file = root.resolve(path).toFile();
        return CompletableFuture.completedFuture(file.exists() ? Optional.of((int) file.length()) : Optional.empty());
    }

    @Override
    public CompletableFuture<Boolean> clear() {
        applyToAll(c -> delete(c));
        return Futures.of(true);
    }

    public void delete(Multihash h) {
        Path path = getFilePath((Cid)h);
        File file = root.resolve(path).toFile();
        if (file.exists())
            file.delete();
    }

    public Optional<Long> getLastAccessTimeMillis(Cid h) {
        Path path = getFilePath(h);
        File file = root.resolve(path).toFile();
        if (! file.exists())
            return Optional.empty();
        try {
            BasicFileAttributes attrs = Files.readAttributes(root.resolve(path), BasicFileAttributes.class);
            FileTime time = attrs.lastAccessTime();
            return Optional.of(time.toMillis());
        } catch (NoSuchFileException nope) {
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void applyToAll(Consumer<Cid> processor) {
        getFilesRecursive(root, processor);
    }

    private void getFilesRecursive(Path path, Consumer<Cid> accumulator) {
        File pathFile = path.toFile();
        if (pathFile.isFile()) {
            accumulator.accept(Cid.decode(pathFile.getName()));
            return;
        }
        else if (!  pathFile.isDirectory())
            throw new IllegalStateException("Specified path "+ path +" is not a file or directory");

        String[] filenames = pathFile.list();
        if (filenames == null)
            throw new IllegalStateException("Couldn't retrieve children of directory: " + path);
        for (String filename : filenames) {
            Path child = path.resolve(filename);
            if (child.toFile().isDirectory()) {
                getFilesRecursive(child, accumulator);
            } else if (filename.startsWith("Q") || filename.startsWith("z")) { // tolerate non content addressed files in the same space
                try {
                    accumulator.accept(Cid.decode(child.toFile().getName()));
                } catch (IllegalStateException e) {
                    // ignore files who's name isn't a valid multihash
                    LOG.info("Ignoring file "+ child +" since name is not a valid multihash");
                }
            }
        }
    }

    private void ensureWithinSizeLimit(long maxSize) {
        if (totalSize.get() <= maxSize)
            return;
        Logging.LOG().info("Starting FileBlockCache reduction");
        AtomicLong toDelete = new AtomicLong(totalSize.get() - (maxSize*9/10));
        SortedMap<Long, Cid> byAccessTime = new TreeMap<>();
        applyToAll(c -> getLastAccessTimeMillis(c).map(t -> byAccessTime.put(t, c)));
        for (Map.Entry<Long, Cid> e : byAccessTime.entrySet()) {
            if (toDelete.get() <= 0)
                break;
            Cid c = e.getValue();
            Optional<Integer> sizeOpt = getSize(c).join();
            if (sizeOpt.isEmpty())
                continue;
            long size = sizeOpt.get();
            delete(c);
            toDelete.addAndGet(-size);
        }
    }
}
