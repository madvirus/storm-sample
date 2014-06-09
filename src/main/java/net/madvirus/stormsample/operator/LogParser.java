package net.madvirus.stormsample.operator;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class LogParser extends BaseFunction {
	private static final Logger LOG = LoggerFactory.getLogger(LogParser.class);

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String log = (String) tuple.getValueByField("logString");
		ShopLog event = parseLog(log);
		collector.emit(Arrays.<Object> asList(event));
		LOG.info("LogParser emit tuple {}", event);
	}

	private ShopLog parseLog(String log) {
		String[] tokens = log.split(",");
		return new ShopLog(tokens[0], Long.parseLong(tokens[1]), Long.parseLong(tokens[2]));
	}

}
