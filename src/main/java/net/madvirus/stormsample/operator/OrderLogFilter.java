package net.madvirus.stormsample.operator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class OrderLogFilter extends BaseFilter {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(OrderLogFilter.class);
	private int partitionIndex;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.info("OrderLogFilter.prepare(): partition[{}/{}]", context.getPartitionIndex(), context.numPartitions());
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String logString = (String) tuple.getValueByField("logString");
		boolean pass = logString.startsWith("ORDER");
		if (!pass)
			LOG.info("OrderLogFilter filtered out {} / {}", logString, partitionIndex);
		return pass;
	}

}
