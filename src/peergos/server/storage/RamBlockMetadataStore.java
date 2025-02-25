package peergos.server.storage;

import peergos.shared.io.ipfs.cid.*;
import peergos.shared.util.*;

import java.util.*;

public class RamBlockMetadataStore implements BlockMetadataStore {

    private final LRUCache<Cid, BlockMetadata> cache;

    public RamBlockMetadataStore() {
        this.cache = new LRUCache<>(50_000);
    }

    @Override
    public Optional<BlockMetadata> get(Cid block) {
        return Optional.ofNullable(cache.get(block));
    }

    @Override
    public void put(Cid block, BlockMetadata meta) {
        cache.put(block, meta);
    }
}
