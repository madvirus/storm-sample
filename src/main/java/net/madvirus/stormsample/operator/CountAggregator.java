package net.madvirus.stormsample.operator;

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class CountAggregator extends BaseAggregator<CountAggregator.State> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(CountAggregator.class);
	private int partitionIndex;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.info("CountAggregator.prepare(): partition[{}/{}]", context.getPartitionIndex(), context.numPartitions());
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public State init(Object batchId, TridentCollector collector) {
		LOG.info("CountAggregator.init({}, collector) / {}", batchId, partitionIndex);
		return new State();
	}

	@Override
	public void aggregate(State val, TridentTuple tuple, TridentCollector collector) {
		val.count++;
	}

	@Override
	public void complete(State val, TridentCollector collector) {
		LOG.info("CountAggregator.complete() / {}", partitionIndex);
		collector.emit(Arrays.<Object> asList(val.count));
	}

	static class State {
		long count = 0;
	}
}
