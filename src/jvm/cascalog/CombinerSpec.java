/*
    Copyright 2010 Nathan Marz
 
    Project and contact information: http://www.cascalog.org/ 

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
