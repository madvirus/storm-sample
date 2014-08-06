package net.madvirus.stormsample.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

public class CountSumBackingMap2 implements IBackingMap<TransactionalValue> {
	private Map<String, CountValue2> sumMap = new ConcurrentHashMap<>();

	@Override
	public List<TransactionalValue> multiGet(List<List<Object>> keys) {
		List<TransactionalValue> values = new ArrayList<>();
		for (List<Object> keyVals : keys) {
			String key = (String) keyVals.get(0);
			CountValue2 countValue = sumMap.get(key);
			if (countValue == null) {
				values.add(null);
			} else {
				values.add(new TransactionalValue<Long>(countValue.getTxId(), countValue.getCount()));
			}
		}
		return values;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<TransactionalValue> vals) {
		for (int i = 0; i < keys.size(); i++) {
			String key = (String) keys.get(i).get(0);
			TransactionalValue<Long> val = vals.get(i);

			CountValue2 countValue = sumMap.get(key);
			if (countValue == null) {
				sumMap.put(key, new CountValue2(val.getTxid(), val.getVal()));
			} else {
				countValue.update(val.getTxid(), val.getVal());
			}
		}

	}

}
