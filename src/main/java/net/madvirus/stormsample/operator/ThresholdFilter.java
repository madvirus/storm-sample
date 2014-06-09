package net.madvirus.stormsample.operator;

import storm.trident.operation.BaseOperation;
import storm.trident.operation.Filter;
import storm.trident.tuple.TridentTuple;

public class ThresholdFilter extends BaseOperation implements Filter {

	private static final long serialVersionUID = 1L;

	private long threshold;

	public ThresholdFilter(long threshold) {
		this.threshold = threshold;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		Long sumValue = tuple.getLongByField("sum");
		return sumValue >= threshold;
	}

}
