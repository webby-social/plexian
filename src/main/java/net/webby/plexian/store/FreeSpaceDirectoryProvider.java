package net.webby.plexian.store;

import java.io.IOException;

public interface FreeSpaceDirectoryProvider {

    public FreeSpaceDirectory getFreeSpaceDirectory() throws IOException;
}
