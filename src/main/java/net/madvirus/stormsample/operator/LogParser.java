package net.madvirus.stormsample.operator;

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class LogParser extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(LogParser.class);
	private int partitionIndex;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.info("LogParser.prepare(): partition[{}/{}]", context.getPartitionIndex(), context.numPartitions());
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String log = tuple.getStringByField("logString");
		ShopLog event = parseLog(log);
		collector.emit(Arrays.<Object> asList(event));
		LOG.info("LogParser emit tuple {} / {}", event, partitionIndex);
	}

	private ShopLog parseLog(String log) {
		String[] tokens = log.split(",");
		return new ShopLog(tokens[0], Long.parseLong(tokens[1]), Long.parseLong(tokens[2]));
	}

}
