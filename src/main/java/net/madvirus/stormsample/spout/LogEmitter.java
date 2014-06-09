package net.madvirus.stormsample.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

public class LogEmitter implements Emitter<Long> {
	private static final Logger LOG = LoggerFactory.getLogger(LogEmitter.class);

	@Override
	public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
		LOG.info("Emitter.emitBatch({}, {}, collector)", tx, coordinatorMeta);
		List<String> logs = getLogs(coordinatorMeta);
		for (String log : logs) {
			List<Object> oneTuple = Arrays.<Object> asList(log);
			collector.emit(oneTuple);
		}
	}

	private List<String> getLogs(Long iteration) {
		List<String> logs = new ArrayList<String>();
		for (int i = 1; i <= 10; i++) {
			long productId = iteration % 3 + i % 4;
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