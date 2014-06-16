package net.madvirus.stormsample.topology;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class CountReducerAggregator implements ReducerAggregator<Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public Long init() {
		return 0L;
	}

	@Override
	public Long reduce(Long curr, TridentTuple tuple) {
		return curr + 1L;
	}

}
