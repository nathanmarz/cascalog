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

import cascading.operation.FilterCall;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class FilterFunctionCall implements FilterCall {
    FunctionCall func;

    public FilterFunctionCall(FunctionCall func) {
        this.func = func;
    }

    public Object getContext() {
        return func.getContext();
    }

    public void setContext(Object o) {
        func.setContext(o);
    }

    public Fields getArgumentFields() {
        return func.getArgumentFields();
    }

    public TupleEntry getArguments() {
        return func.getArguments();
    }


}