package cascalog.elephantdb;

import cascalog.Util;
import elephantdb.document.KeyValDocument;
import elephantdb.persistence.Persistence;

import java.io.IOException;

/** User: sritchie Date: 1/4/12 Time: 1:21 PM */
public class KeyValIndexer extends ClojureIndexer {

    public KeyValIndexer(Object[] spec) {
        super(spec);
    }

    public void index(Persistence lp, Object document) throws IOException {
        if(this.fn==null)
            this.fn = Util.bootFn(this.spec);

        KeyValDocument doc = (KeyValDocument) document;

        try {
            this.fn.invoke(lp, doc.key, doc.value);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
