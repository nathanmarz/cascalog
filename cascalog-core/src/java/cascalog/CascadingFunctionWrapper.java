/*
Copyright 2012 Twitter, Inc.

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
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

/**
 *
 */
public class CascadingFunctionWrapper<T> extends BaseOperation<T> implements Function<T> {
  private final Function<T> func;

  public CascadingFunctionWrapper(Fields fields, Function<T> func) {
    super(func.getNumArgs(), fields);
    this.func = func;
  }

  public void operate(FlowProcess flowProcess, FunctionCall<T> tFunctionCall) {
    func.operate(flowProcess, tFunctionCall);
  }

  public void prepare(FlowProcess flowProcess, OperationCall<T> tOperationCall) {
    func.prepare(flowProcess, tOperationCall);
  }

  public void flush(FlowProcess flowProcess, OperationCall<T> tOperationCall) {
    func.flush(flowProcess, tOperationCall);
  }

  public void cleanup(FlowProcess flowProcess, OperationCall<T> tOperationCall) {
    func.cleanup(flowProcess, tOperationCall);
  }

  public boolean isSafe() {
    return func.isSafe();
  }
}
