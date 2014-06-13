package net.madvirus.stormsample.operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class CountSumFunction implements Function {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(CountSumFunction.class);

	private Map<String, Long> sumMap;

	private int partitionIndex;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.info("CountSumFunction.prepare(): partition[{}/{}]", context.getPartitionIndex(), context.numPartitions());
		sumMap = new HashMap<String, Long>();
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String groupKey = tuple.getStringByField("productId:time");
		Long count = tuple.getLongByField("count");
		LOG.info("Grouped Tuple : {} / {}", tuple, partitionIndex);
		long sum = count;
		if (sumMap.containsKey(groupKey)) {
			sum += sumMap.get(groupKey);
		}
		sumMap.put(groupKey, sum);
		collector.emit(Arrays.<Object> asList(sum));
	}

}
