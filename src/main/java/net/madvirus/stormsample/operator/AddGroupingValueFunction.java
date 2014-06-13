package net.madvirus.stormsample.operator;

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseOperation;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class AddGroupingValueFunction extends BaseOperation implements Function {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AddGroupingValueFunction.class);

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.info("AddGroupingValueFunction.prepare(): partition[{}/{}]", context.getPartitionIndex(), context.numPartitions());
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		ShopLog shopLog = (ShopLog) tuple.getValueByField("shopLog");
		long time = shopLog.getTimestamp() / (1000L * 60L);
		collector.emit(Arrays.<Object> asList(shopLog.getProductId() + ":" + time));
	}

}
