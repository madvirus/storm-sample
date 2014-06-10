package net.madvirus.stormsample.operator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseOperation;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class ThresholdFilter extends BaseOperation implements Filter {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ThresholdFilter.class);
	private long threshold;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		Long value = (Long) conf.get("ThresholdFilter.value");
		threshold = value.longValue();
		LOG.info("ThresholdFilter.prepare(): threshold = {}", value);
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		Long sumValue = tuple.getLongByField("sum");
		return sumValue >= threshold;
	}

}
