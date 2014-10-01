package peergos.user.fs;

import peergos.crypto.SymmetricKey;
import peergos.crypto.SymmetricLocationLink;
import peergos.user.UserContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileWrapper
{
    private UserContext context;
    private SymmetricKey baseKey;
    private Metadata first;
    private FileProperties props;

    public FileWrapper(UserContext context, Metadata first, SymmetricKey baseKey) {
        this.context = context;
        this.first = first;
        this.baseKey = baseKey;
        this.props = (FileProperties) first.getProps(baseKey);
    }

    private List<Metadata> getAllChunkMetadata(SymmetricKey baseKey) throws IOException {
        Location next = first.getNextChunkLocation(baseKey, null);
        List<Metadata> res = new ArrayList();
        res.add(first);
        while (next != null) {
            Metadata meta = context.getMetadata(next, baseKey);
            res.add(meta);
        }
        return res;
    }

    public boolean isDir() {
        return first instanceof DirAccess;
    }

    public FileProperties props() {
        return props;
    }

    public List<FileWrapper> getChildren() throws IOException {
        if (!isDir())
            return new ArrayList(0);
        DirAccess meta = (DirAccess)first;
        Map<SymmetricLocationLink, Metadata> files = context.retrieveMetadata(meta.getFiles(), baseKey);
        List<FileWrapper> res = new ArrayList(files.size());
        for (SymmetricLocationLink loc: files.keySet()) {
            res.add(new FileWrapper(context, files.get(loc), loc.target(baseKey)));
        }
        return res;
    }
}
