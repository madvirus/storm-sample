package net.madvirus.stormsample.topology;

import net.madvirus.stormsample.operator.LogParser;
import net.madvirus.stormsample.operator.OrderLogFilter;
import net.madvirus.stormsample.operator.TimeAppendFunction;
import net.madvirus.stormsample.spout.LogSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class LogTopology {

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("log", new LogSpout());
		stream
				.each(new Fields("logString"), new OrderLogFilter())
				.each(new Fields("logString"), new LogParser(), new Fields("shopLog"))
				.each(new Fields("shopLog"), new TimeAppendFunction(), new Fields("time"))
		// .aggregate(new Fields(""), agg, functionFields);
		;
		StormTopology stormTopology = topology.build();

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("cdc", conf, stormTopology);

		int iteration = 0;

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		cluster.shutdown();
	}

}
