package net.madvirus.stormsample.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.madvirus.stormsample.operator.AddGroupingValueFunction;
import net.madvirus.stormsample.operator.LogParser;
import net.madvirus.stormsample.operator.OrderLogFilter;
import net.madvirus.stormsample.operator.ShopLog;
import net.madvirus.stormsample.operator.Util;
import net.madvirus.stormsample.spout.LogSpout;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;

public class LogTopology5PartitionPersist {

    public static class DistinctState implements State {

        private Map<String, ShopLog> map = new ConcurrentHashMap<>();

        private Map<String, ShopLog> temp = new ConcurrentHashMap<>();

        @Override
        public void beginCommit(Long txid) {
            temp.clear();
        }

        @Override
        public void commit(Long txid) {
            for (Map.Entry<String, ShopLog> entry : temp.entrySet())
                map.put(entry.getKey(), entry.getValue());
        }

        public boolean hasKey(String key) {
            return map.containsKey(key) || temp.containsKey(key);
        }

        public void put(String key, ShopLog value) {
            temp.put(key, value);
        }

    }

    public static class DistinctStateFactory implements StateFactory {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("rawtypes")
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new DistinctState();
        }
    }

    @SuppressWarnings("serial")
    public static class DistinctStateUpdater extends BaseStateUpdater<DistinctState> {

        @Override
        public void updateState(DistinctState state, List<TridentTuple> tuples, TridentCollector collector) {
            List<TridentTuple> newEntries = new ArrayList<>();
            for (TridentTuple t : tuples) {
                String key = t.getStringByField("productId:time");
                if (!state.hasKey(key)) {
                    state.put(key, (ShopLog) t.getValueByField("shopLog"));
                    newEntries.add(t);
                }
            }
            for (TridentTuple t : newEntries) {
                collector.emit(Arrays.asList(t.getValueByField("shopLog"), t.getStringByField("productId:time")));
            }
        }
    }

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        topology.newStream("log", new LogSpout())
                .each(new Fields("logString"), new OrderLogFilter())
                .each(new Fields("logString"), new LogParser(), new Fields("shopLog"))
                .each(new Fields("shopLog"), new AddGroupingValueFunction(), new Fields("productId:time"))
                .partitionPersist(new DistinctStateFactory(), new Fields("shopLog", "productId:time"), new DistinctStateUpdater(),
                        new Fields("shopLog", "productId:time"))
                .newValuesStream()
                .each(new Fields("shopLog", "productId:time"), Util.printer());

        StormTopology stormTopology = topology.build();

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", conf, stormTopology);

        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
        }
        cluster.shutdown();
    }

}
