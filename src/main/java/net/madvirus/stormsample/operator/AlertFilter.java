package net.madvirus.stormsample.operator;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseOperation;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class AlertFilter extends BaseOperation implements Filter {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AlertFilter.class);

	private Map<String, Boolean> aleadyNotiKey = new HashMap<>();
	private int partitionIndex;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.info("AlertFilter.prepare(): partition[{}/{}]", context.getPartitionIndex(), context.numPartitions());
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		LOG.info("AlertFilter.isKeep({})/{}", tuple, partitionIndex);
		String groupKey = tuple.getStringByField("productId:time");
		if (!aleadyNotiKey.containsKey(groupKey)) {
			LOG.error("!!!!!!! " + tuple);
			aleadyNotiKey.put(groupKey, Boolean.TRUE);
		}
		return false;
	}

}
