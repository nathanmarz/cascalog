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

package cascading.pipe.cogroup;

import cascading.tuple.Tuple;
import java.util.Iterator;
import java.util.List;


public class CascalogJoiner implements Joiner {
    public static enum JoinType {
        INNER,
        OUTER,
        EXISTS;
    }

    private List<JoinType> joins;

    public CascalogJoiner(List<JoinType> joins) {
        this.joins = joins;
    }


    public Iterator<Tuple> getIterator(GroupClosure closure) {
        return new JoinIterator(closure);
    }

    public int numJoins() {
        return joins.size()-1;
    }

    protected class JoinIterator extends OuterJoin.JoinIterator {
        public JoinIterator(GroupClosure closure) {
            super(closure);
        }

        @Override
        protected boolean isOuter(int i) {
          return joins.get(i)!=JoinType.INNER && super.isOuter( i );
        }

        @Override
        protected Iterator getIterator(int i) {
            if(joins.get(i)==JoinType.EXISTS) {
                final boolean isEmpty = ((CoGroupClosure) closure).isEmpty(i);
                final Iterator wrapped = super.getIterator(i);
                return new Iterator() {
                    private boolean emittedOne = false;

                    public boolean hasNext() {
                        return !emittedOne && wrapped.hasNext();
                    }

                    public Object next() {
                        if(emittedOne) throw new RuntimeException("Shouldn't be accessing outerjoin_first more than once");
                        emittedOne = true;
                        Tuple t = (Tuple) wrapped.next();
                        Tuple ret = new Tuple();
                        for(int i=0; i<t.size(); i++) {
                            ret.add(!isEmpty);
                        }
                        return ret;
                    }

                    public void remove() {
                        //not implemented
                    }

                };
            } else {
                return super.getIterator(i);
            }
        }
    }
}
