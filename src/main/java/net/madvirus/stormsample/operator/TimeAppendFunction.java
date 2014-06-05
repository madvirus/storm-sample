package net.madvirus.stormsample.operator;

import java.util.Arrays;

import storm.trident.operation.BaseOperation;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TimeAppendFunction extends BaseOperation implements Function {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		ShopLog shopLog = (ShopLog) tuple.getValueByField("shopLog");
		long time = shopLog.getTimestamp() % (1000L * 60L);
		collector.emit(Arrays.<Object> asList(time));
	}

}
