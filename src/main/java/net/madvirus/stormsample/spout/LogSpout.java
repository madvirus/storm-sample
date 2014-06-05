package net.madvirus.stormsample.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public class LogSpout implements ITridentSpout<Long> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
		return new LogBatchCoordinator();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
		return new LogEmitter();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("logString");
	}

	private static class LogBatchCoordinator implements BatchCoordinator<Long> {
		private static final Logger LOG = LoggerFactory.getLogger(LogBatchCoordinator.class);

		@Override
		public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
			LOG.info("BatchCoordinator.initializeTransaction({}, {}, {})", new Object[] { txid, prevMetadata, currMetadata });
			if (prevMetadata == null) {
				return 1L;
			}
			return prevMetadata + 1L;
		}

		@Override
		public void success(long txid) {
			LOG.info("BatchCoordinator.success({})", txid);
		}

		@Override
		public boolean isReady(long txid) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
			LOG.info("BatchCoordinator.isReady({})", new Object[] { txid });
			return true;
		}

		@Override
		public void close() {
			LOG.info("BatchCoordinator.close()");
		}
	}

	private static class LogEmitter implements Emitter<Long> {
		private static final Logger LOG = LoggerFactory.getLogger(LogEmitter.class);

		@Override
		public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
			LOG.info("Emitter.emitBatch({}, {}, collector)", tx, collector);
			List<String> logs = getLogs(coordinatorMeta);
			for (String log : logs) {
				List<Object> oneTuple = Arrays.<Object> asList(log);
				collector.emit(oneTuple);
			}
		}

		private List<String> getLogs(Long iteration) {
			List<String> logs = new ArrayList<String>();
			for (int i = 1; i <= 10; i++) {
				long productId = iteration % 3 + i;
				String type = (iteration + i) % 10 != 0 ? "ORDER" : "VISIT";
				String log = type + "," + productId + "," + System.currentTimeMillis();
				logs.add(log);
			}
			return logs;
		}

		@Override
		public void success(TransactionAttempt tx) {
			LOG.info("Emitter.success({})", tx);
		}

		@Override
		public void close() {
			LOG.info("Emitter.close()");
		}

	}
}
