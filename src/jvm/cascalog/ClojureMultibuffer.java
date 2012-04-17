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

import cascading.tuple.Fields;
import cascalog.MultiGroupBy.MultiBuffer;
import cascalog.MultiGroupBy.MultiBufferContext;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.RT;

import java.util.ArrayList;
import java.util.List;

public class ClojureMultibuffer extends ClojureCascadingBase implements MultiBuffer {

    public ClojureMultibuffer(Fields fields, IFn fn) {
        super(fields, fn);
    }

    public void operate(MultiBufferContext context) {
        List inputTuples = new ArrayList();
        for (int i = 0; i < context.size(); i++) {
            inputTuples.add(IteratorSeq
                .create(new RegularTupleSeqConverter(context.getArgumentsIterator(i))));
        }

        ISeq result_seq = RT.seq(applyFunction(RT.seq(inputTuples)));
        while (result_seq != null) {
            Object obj = result_seq.first();
            context.emit(Util.coerceToTuple(obj));
            result_seq = result_seq.next();
        }
    }
}
