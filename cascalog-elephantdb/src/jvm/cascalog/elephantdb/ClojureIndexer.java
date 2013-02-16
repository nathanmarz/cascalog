package cascalog.elephantdb;

import cascalog.Util;
import clojure.lang.IFn;
import elephantdb.index.Indexer;
import elephantdb.persistence.Persistence;

import java.io.IOException;

public class ClojureIndexer implements Indexer {
    Object[] spec;
    transient IFn fn = null;

    public ClojureIndexer(Object[] spec) {
        this.spec = spec;
    }

    public void index(Persistence lp, Object doc) throws IOException {
        if(this.fn==null)
            this.fn = Util.bootFn(this.spec);
        
        try {
            this.fn.invoke(lp, doc);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
