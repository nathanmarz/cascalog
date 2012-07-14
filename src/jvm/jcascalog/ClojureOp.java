package jcascalog;

import java.util.ArrayList;
import java.util.List;

import jcascalog.fluent.op.Op;
import cascalog.Util;

public class ClojureOp implements Op {
	String _namespace;
	String _name;
	List<Object> _hofArgs;

	public ClojureOp(String namespace, String name, List<Object> hofArgs) {
		_namespace = namespace;
		_name = name;
		_hofArgs = hofArgs;
	}

	public ClojureOp(String namespace, String name) {
		this(namespace, name, null);
	}

	public List<Object> toRawCascalogPredicate(List<Object> fieldsDeclaration) {
		List<Object> pred = new ArrayList<Object>();
		pred.add(Util.getVar(_namespace, _name)); // the op
		List<Object> hofAndFields = new ArrayList<Object>(fieldsDeclaration);
		if (_hofArgs != null)
			hofAndFields.add(0, _hofArgs);
		pred.add(hofAndFields);
		return pred;
	}
}
