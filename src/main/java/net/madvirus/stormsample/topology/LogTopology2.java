package net.madvirus.stormsample.topology;

import net.madvirus.stormsample.operator.AddGroupingValueFunction;
import net.madvirus.stormsample.operator.AlertFilter;
import net.madvirus.stormsample.operator.CountAggregator;
import net.madvirus.stormsample.operator.CountSumFunction;
import net.madvirus.stormsample.operator.LogParser;
import net.madvirus.stormsample.operator.OrderLogFilter;
import net.madvirus.stormsample.operator.ThresholdFilter;
import net.madvirus.stormsample.spout.LogSpout;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class LogTopology2 {

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		topology.newStream("log", new LogSpout())
				.parallelismHint(1)
				.shuffle()
				.each(new Fields("logString"), new OrderLogFilter())
				.each(new Fields("logString"), new LogParser(), new Fields("shopLog"))
				.each(new Fields("shopLog"), new AddGroupingValueFunction(), new Fields("productId:time"))
				.parallelismHint(2)
				.groupBy(new Fields("productId:time"))
				.aggregate(new CountAggregator(), new Fields("count"))
				.parallelismHint(1).partitionBy(new Fields("productId:time"))
				.each(new Fields("productId:time", "count"), new CountSumFunction(), new Fields("sum"))
				.parallelismHint(3).shuffle()
				.each(new Fields("productId:time", "sum"), new ThresholdFilter())
				.each(new Fields("productId:time", "sum"), new AlertFilter())
				.parallelismHint(1)
				;
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
