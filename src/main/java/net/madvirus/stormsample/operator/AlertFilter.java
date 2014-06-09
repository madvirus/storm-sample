package net.madvirus.stormsample.operator;

import java.util.HashMap;
import java.util.Map;

import storm.trident.operation.BaseOperation;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class AlertFilter extends BaseOperation implements Filter {

	private static final long serialVersionUID = 1L;

	private Map<String, Boolean> aleadyNotiKey = new HashMap<>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String groupKey = tuple.getStringByField("productId:time");
		if (!aleadyNotiKey.containsKey(groupKey)) {
			System.err.println("!!!!!!! " + tuple);
			aleadyNotiKey.put(groupKey, Boolean.TRUE);
		}
		return false;
	}

}
