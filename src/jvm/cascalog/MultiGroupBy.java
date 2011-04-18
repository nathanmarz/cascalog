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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import cascading.operation.BaseOperation;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.cogroup.GroupClosure;
import cascading.pipe.cogroup.Joiner;
import cascading.tuple.Fields;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.Tuple;

public class MultiGroupBy extends SubAssembly {
  public static interface MultiBuffer extends Serializable {
      void operate(MultiBufferContext context);
  }

  public static class MultiBufferContext {
      GroupClosure _closure;
      SpillableTupleList _results = new SpillableTupleList();
      int _pipeFieldsSum;

      public MultiBufferContext(GroupClosure closure, int pipeFieldsSum) {
          _closure = closure;
          _pipeFieldsSum = pipeFieldsSum;
      }

      public int size() {
          return _closure.size();
      }

      public void emit(Tuple result) {
          Tuple ret = new Tuple(_closure.getGrouping());
          ret.addAll(result);
          while(ret.size() < _pipeFieldsSum) {
            ret.add(0);
          }
          _results.add(ret);
      }

      public Iterator<Tuple> getArgumentsIterator(int pos) {
          return _closure.getIterator(pos);
      }

      public SpillableTupleList getResults() {
          return _results;
      }
  }
  
  public static class MultiBufferExecutor implements Serializable {
    private MultiBuffer _buffer;
    private MultiBufferContext _context;
    private int _pipeFieldsSum;
    private GroupClosure _closure;
    
    public MultiBufferExecutor(MultiBuffer buffer, int pipeFieldsSum) {
        _buffer = buffer;
        _pipeFieldsSum = pipeFieldsSum;
    }
        
    public void setContext(GroupClosure closure) {
       _closure = closure;
       _context = new MultiBufferContext(closure, _pipeFieldsSum);
    }
    
    public SpillableTupleList getResults() {
      return _context.getResults();
    }

    public void operate() {
        ((BaseOperation) _buffer).prepare(_closure.getFlowProcess(), null);
        _buffer.operate(_context);
        ((BaseOperation) _buffer).cleanup(_closure.getFlowProcess(), null);
    }            
  }
  
  
  protected static class MultiGroupJoiner implements Joiner {

    protected MultiBufferExecutor _buffer;
    
    public MultiGroupJoiner(int pipeFieldsSum, MultiBuffer buffer) {
      _buffer = new MultiBufferExecutor(buffer, pipeFieldsSum);
    }
    
    public Iterator<Tuple> getIterator(GroupClosure closure) {
      _buffer.setContext(closure);
      _buffer.operate();
      return _buffer.getResults().iterator();
    }

    public int numJoins() {
      return -1;      
    }
  }
  
  public MultiGroupBy(Pipe p0, Pipe p1, Fields groupFields, int pipeFieldsSum, MultiBuffer operation) {
    Pipe[] pipes = new Pipe[] { p0, p1};
    Fields[] fields = new Fields[] {groupFields, groupFields};
    init(pipes, fields, pipeFieldsSum, groupFields, operation);
  }
  
  public MultiGroupBy(Pipe p0, Fields group0, Pipe p1, Fields group1, int pipeFieldsSum, Fields groupRename, MultiBuffer operation) {
    Pipe[] pipes = new Pipe[] { p0, p1};
    Fields[] fields = new Fields[] {group0, group1};
    init(pipes, fields, pipeFieldsSum, groupRename, operation);
  }
  
  public MultiGroupBy(Pipe[] pipes, Fields groupFields, int pipeFieldsSum, MultiBuffer operation) {
    Fields[] allGroups = new Fields[pipes.length];
    Arrays.fill(allGroups, groupFields);
    init(pipes, allGroups, pipeFieldsSum, groupFields, operation);
  }
  
  public MultiGroupBy(Pipe[] pipes, Fields[] groupFields, int pipeFieldsSum, Fields groupingRename, MultiBuffer operation) {
    init(pipes, groupFields, pipeFieldsSum, groupingRename, operation);
  }
  
  protected void init(Pipe[] pipes, Fields[] groupFields, int pipeFieldsSum, Fields groupingRename, MultiBuffer operation) {
 
    Fields resultFields =  Fields.join(groupingRename, ((BaseOperation)operation).getFieldDeclaration());
    if(resultFields.size()>pipeFieldsSum) throw new IllegalArgumentException("Can't have output more than sum of input pipes since this is a hack!");    
    
    // unfortunately, need to hack around CoGroup validation stuff since cascading assumes it will return #fields=sum of input pipes
    Fields fake = new Fields();
    fake = fake.append(resultFields);
    int i=0;
    while(fake.size() < pipeFieldsSum) {
      fake = fake.append(new Fields("__" + i));
      i++;
    }
    Pipe result = new CoGroup(pipes, groupFields, fake, new MultiGroupJoiner(pipeFieldsSum, operation));
    result = new Each(result, resultFields, new Identity());
    setTails(result);
  }
}