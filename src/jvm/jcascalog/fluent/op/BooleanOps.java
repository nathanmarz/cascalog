package jcascalog.fluent.op;

import jcascalog.op.Equals;
import jcascalog.op.GT;
import jcascalog.op.GTE;
import jcascalog.op.LT;
import jcascalog.op.LTE;

public class BooleanOps {

	public static BooleanOp equals() {
		return new Equals();
	}

	public static BooleanOp gt() {
		return new GT();
	}

	public static BooleanOp gte() {
		return new GTE();
	}

	public static BooleanOp lt() {
		return new LT();
	}

	public static BooleanOp lte() {
		return new LTE();
	}

}
