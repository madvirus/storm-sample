package net.madvirus.stormsample.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.OpaqueValue;
import storm.trident.state.map.IBackingMap;

@SuppressWarnings("rawtypes")
public class CountSumBackingMap implements IBackingMap<OpaqueValue> {
	private static final Logger LOG = LoggerFactory.getLogger(CountSumBackingMap.class);
	private Map<String, CountValue> sumMap = new ConcurrentHashMap<>();

	@Override
	public List<OpaqueValue> multiGet(List<List<Object>> keys) {
		List<OpaqueValue> values = new ArrayList<>();
		for (List<Object> keyVals : keys) {
			String key = (String) keyVals.get(0);
			CountValue countValue = sumMap.get(key);
			if (countValue == null) {
				values.add(null);
			} else {
				values.add(new OpaqueValue<Long>(countValue.getCurrentTxId(), countValue.getCurrCount(), countValue.getPrevCount()));
			}
		}
		LOG.info("multiGet({}) = {}", keys, toString(values));
		return values;
	}

	private String toString(List<OpaqueValue> values) {
		String result = "[";
		boolean first = true;
		for (OpaqueValue ov : values) {
			if (!first) {
				result += ", ";
			}
			first = false;
			if (ov == null)
				result += "null";
			else
				result += toString(ov);
		}
		return result + "]";
	}

	private String toString(OpaqueValue ov) {
		return "(currTxid=" + ov.getCurrTxid() + ",prev=" + ov.getPrev() + ",curr=" + ov.getCurr() + ")";
	}

	@SuppressWarnings("unchecked")
	@Override
	public void multiPut(List<List<Object>> keys, List<OpaqueValue> vals) {
		for (int i = 0; i < keys.size(); i++) {
			String key = (String) keys.get(i).get(0);
			OpaqueValue<Long> val = vals.get(i);
			LOG.info("multiPut(); key = {}, val = {}", key, toString(val));

			CountValue countValue = sumMap.get(key);
			if (countValue == null) {
				sumMap.put(key, new CountValue(val.getCurrTxid(), val.getPrev(), val.getCurr()));
			} else {
				countValue.update(val.getCurrTxid(), val.getPrev(), val.getCurr());
			}
		}
	}

}
