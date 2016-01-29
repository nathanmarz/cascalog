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

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import clojure.lang.Associative;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;

public class ClojureCascadingBase extends BaseOperation {
  private byte[] serializedFn;
  protected IFn fn;
  protected IFn cleanupFn;
  protected Associative bindingMap;

  public void initialize(IFn fn) {
    serializedFn = Util.serializeFn(fn);
  }

  public ClojureCascadingBase(IFn fn) {
    initialize(fn);
  }

  public ClojureCascadingBase(Fields fields, IFn fn) {
    super(fields);
    initialize(fn);
  }

  @Override
  public void prepare(FlowProcess fp, OperationCall call) {
    this.bindingMap = PersistentHashMap
      .create(Util.getVar("cascalog.cascading.stats", "*flow-process*"), fp,
              Util.getVar("cascalog.cascading.stats", "*op-call*"), call);

    IFn fn = Util.deserializeFn(serializedFn);

    Boolean isPrepared =
      (Boolean) Util.bootSimpleFn("cascalog.cascading.def", "prepared?").invoke(fn);

    Var.pushThreadBindings(bindingMap);
    try {
      if (isPrepared) {
        Object res = fn.invoke(fp, call);

        if(res instanceof Map) {
          Map resmap = (Map) res;
          this.fn = (IFn) resmap.get(Keyword.intern("operate"));
          this.cleanupFn = (IFn) resmap.get(Keyword.intern("cleanup"));
        } else {
          this.fn = (IFn) res;
          this.cleanupFn = null;
        }
      } else {
        this.fn = fn;
        this.cleanupFn = null;
      }
    } finally {
      Var.popThreadBindings();
    }
  }

  protected Object applyFunction(ISeq seq) {
    return this.fn.applyTo(seq);
  }

  protected Object invokeFunction(Object arg) {
    return this.fn.invoke(arg);
  }

  protected Object invokeFunction() {
    return this.fn.invoke();
  }


  @Override
  public void cleanup(FlowProcess flowProcess, OperationCall call) {
    super.cleanup(flowProcess, call);

    if(cleanupFn != null) {
      Var.pushThreadBindings(bindingMap);
      try {
        cleanupFn.invoke();
      } finally {
         Var.popThreadBindings();
      }
    }
  }
}
