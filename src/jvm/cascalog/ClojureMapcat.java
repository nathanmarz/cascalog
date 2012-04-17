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

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

public class ClojureMapcat extends ClojureCascadingBase implements Function {

    public ClojureMapcat(Fields fields, IFn fn) {
        super(fields, fn);
    }

    public void operate(FlowProcess flow_process, FunctionCall fn_call) {
        ISeq fn_args_seq = Util.coerceFromTuple(fn_call.getArguments().getTuple());
        ISeq result_seq = RT.seq(applyFunction(fn_args_seq));
        TupleEntryCollector collector = fn_call.getOutputCollector();
        while (result_seq != null) {
            Object obj = result_seq.first();
            collector.add(Util.coerceToTuple(obj));
            result_seq = result_seq.next();
        }
    }
}
