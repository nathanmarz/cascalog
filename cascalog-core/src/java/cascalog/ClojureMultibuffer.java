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

import java.util.ArrayList;
import java.util.List;

import cascading.tuple.Fields;
import cascalog.MultiGroupBy.MultiBuffer;
import cascalog.MultiGroupBy.MultiBufferContext;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.RT;
import clojure.lang.Var;

public class ClojureMultibuffer extends ClojureCascadingBase implements MultiBuffer {

  public ClojureMultibuffer(Fields outputFields, IFn fn) {
    super(outputFields, fn);
  }

  public void operate(MultiBufferContext context) {
    List inputTuples = new ArrayList();

    for (int i = 0; i < context.size(); i++) {
      inputTuples
          .add(IteratorSeq.create(new RegularTupleSeqConverter(context.getArgumentsIterator(i))));
    }

    Var.pushThreadBindings(bindingMap);
    try {
      ISeq resultSeq = RT.seq(applyFunction(RT.seq(inputTuples)));
      while (resultSeq != null) {
        Object obj = resultSeq.first();
        context.emit(Util.coerceToTuple(obj));
        resultSeq = resultSeq.next();
      }


    } finally {
      Var.popThreadBindings();
    }
  }
}
