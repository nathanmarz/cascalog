package jcascalog.fluent.op;

import jcascalog.op.Count;
import jcascalog.op.DistinctCount;
import jcascalog.op.FixedSample;
import jcascalog.op.Limit;
import jcascalog.op.LimitRank;

public class SpecialOps extends NonBooleanOps {

	public static SpecialOp count() {
		return new Count();
	}

	public static SpecialOp distinctCount() {
		return new DistinctCount();
	}

	public static SpecialOp fixedSample(int amount) {
		return new FixedSample(amount);
	}

	public static SpecialOp limit(int amount) {
		return new Limit(amount);
	}

	public static SpecialOp limitRank(int amount) {
		return new LimitRank(amount);
	}

}
