package net.madvirus.stormsample.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseOperation;
import storm.trident.operation.Filter;
import storm.trident.tuple.TridentTuple;

public class OrderLogFilter extends BaseOperation implements Filter {
	private static final Logger LOG = LoggerFactory.getLogger(OrderLogFilter.class);
	private static final long serialVersionUID = 1L;

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String logString = (String) tuple.getValueByField("logString");
		boolean orderLog = logString.startsWith("ORDER");
		if (!orderLog)
			LOG.info("OrderLogFilter filtered out {}", logString);
		return orderLog;
	}

}
