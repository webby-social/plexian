package net.webby.plexian.indexator.lucene;

import java.io.IOException;

public interface LuceneIndexProvider {

    public <T> T doInIndex(String entity, IndexReaderAware<T> accessor) throws IOException;
}
