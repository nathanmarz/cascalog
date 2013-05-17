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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;

public class ClojureCascadingBase extends BaseOperation {
  private byte[] serializedFn;
  protected IFn fn;

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
    this.fn = Util.deserializeFn(serializedFn);
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
  }
}
