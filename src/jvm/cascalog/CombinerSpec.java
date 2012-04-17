/*
    Copyright 2010 Nathan Marz
 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package cascalog;

import clojure.lang.IFn;
import java.io.Serializable;

public class CombinerSpec implements Serializable {
    private IFn init;
    private IFn combiner;
    private IFn extractor = null;
    
    private byte[] init_spec_ser;
    private byte[] combiner_spec_ser;
    private byte[] extractor_spec_ser = null;

    public CombinerSpec(IFn init_spec, IFn combiner_spec) {
        this(init_spec, combiner_spec, null);
    }

    public CombinerSpec(IFn init_spec, IFn combiner_spec, IFn extractor_spec) {
        this.init_spec_ser = Util.serializeFn(init_spec);
        this.combiner_spec_ser = Util.serializeFn(combiner_spec);
        if(extractor_spec !=null)
            this.extractor_spec_ser = Util.serializeFn(extractor_spec);
    }
    
    public void prepare() {
        this.init = Util.deserializeFn(init_spec_ser);
        this.combiner = Util.deserializeFn(combiner_spec_ser);
        if(extractor_spec_ser!=null)
            this.extractor = Util.deserializeFn(extractor_spec_ser);
    }
    
    public IFn getInit() {
        return init;
    }

    public IFn getCombiner() {
        return combiner;
    }
    
    public IFn getExtractor() {
        return extractor;
    }
}
