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

import java.util.List;

import cascading.flow.FlowProcess;
import cascalog.aggregator.CombinerSpec;
import clojure.lang.IFn;
import clojure.lang.RT;

public class ClojureParallelAgg implements ParallelAgg {
  CombinerSpec _spec;
  IFn _initFn;
  IFn _combinerFn;

  public ClojureParallelAgg(CombinerSpec spec) {
    _spec = spec;
  }

  // TODO: Remove this once we have a functor properly in place.
  public void prepare(FlowProcess flowProcess) {
    _initFn = Util.deserializeFn(_spec.prepareFn);
    _combinerFn = Util.deserializeFn(_spec.combineFn);
  }

  public List<Object> init(List<Object> input) {
    return Util.coerceToList(_initFn.applyTo(RT.seq(input)));
  }

  public List<Object> combine(List<Object> val1, List<Object> val2) {
    return Util.coerceToList(_combinerFn.applyTo(Util.cat(RT.seq(val1), RT.seq(val2))));
  }

}
