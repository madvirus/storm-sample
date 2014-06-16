package net.madvirus.stormsample.state;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.OpaqueMap;
import backtype.storm.task.IMetricsContext;

public class CountSumStateFactory implements StateFactory {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return OpaqueMap.build(new CountSumBackingMap());
	}

}
