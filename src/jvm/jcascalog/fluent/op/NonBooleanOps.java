package jcascalog.fluent.op;

import java.util.regex.Pattern;

import jcascalog.op.Avg;
import jcascalog.op.Div;
import jcascalog.op.Max;
import jcascalog.op.Min;
import jcascalog.op.Minus;
import jcascalog.op.Multiply;
import jcascalog.op.Plus;
import jcascalog.op.ReParse;
import jcascalog.op.Sum;

public class NonBooleanOps extends BooleanOps {

	public static NonBooleanOp avg() {
		return new Avg();
	}

	public static NonBooleanOp div() {
		return new Div();
	}

	public static NonBooleanOp max() {
		return new Max();
	}

	public static NonBooleanOp min() {
		return new Min();
	}

	public static NonBooleanOp minus() {
		return new Minus();
	}

	public static NonBooleanOp multiply() {
		return new Multiply();
	}

	public static NonBooleanOp plus() {
		return new Plus();
	}

	// TODO is there any advantage to ask for a Pattern?
	// worst case we could propose 2 methods
	public static NonBooleanOp reParse(String pattern) {
		return new ReParse(Pattern.compile(pattern));
	}

	public static NonBooleanOp sum() {
		return new Sum();
	}

}
