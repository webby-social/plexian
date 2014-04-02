package net.sf.plexian.indexator.lucene;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

public interface IndexReaderAware<T> {

    public T invoke(IndexReader reader) throws IOException;
}
