package net.madvirus.stormsample.topology;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import net.madvirus.stormsample.operator.AddGroupingValueFunction;
import net.madvirus.stormsample.operator.LogParser;
import net.madvirus.stormsample.operator.OrderLogFilter;
import net.madvirus.stormsample.operator.ShopLog;
import net.madvirus.stormsample.operator.Util;
import net.madvirus.stormsample.spout.LogSpout;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class LogTopology4Aggregate {

	public static class DistinctAggregator extends BaseAggregator<Map<String, ShopLog>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Map<String, ShopLog> init(Object batchId, TridentCollector collector) {
			return new ConcurrentHashMap<>();
		}

		@Override
		public void aggregate(Map<String, ShopLog> val, TridentTuple tuple, TridentCollector collector) {
			String key = tuple.getStringByField("productId:time");
			if (val.containsKey(key))
				return;

			val.put(key, (ShopLog) tuple.getValueByField("shopLog"));
		}

		@Override
		public void complete(Map<String, ShopLog> val, TridentCollector collector) {
			for (Entry<String, ShopLog> entry : val.entrySet()) {
				collector.emit(Arrays.<Object> asList(entry.getKey(), entry.getValue()));
			}
		}
	}

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		topology.newStream("log", new LogSpout())
				.each(new Fields("logString"), new OrderLogFilter())
				.each(new Fields("logString"), new LogParser(), new Fields("shopLog"))
				.each(new Fields("shopLog"), new AddGroupingValueFunction(), new Fields("productId:time"))
				.aggregate(new Fields("shopLog", "productId:time"), new DistinctAggregator(), new Fields("shopLog", "productId:time"))
				.each(new Fields("shopLog", "productId:time"), Util.printer());

		StormTopology stormTopology = topology.build();

		Config conf = new Config();
		conf.put("ThresholdFilter.value", 5L);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("cdc", conf, stormTopology);

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
		}
		cluster.shutdown();
	}

}
