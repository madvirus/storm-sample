package net.madvirus.stormsample.spout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ITridentSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

@SuppressWarnings("rawtypes")
public class LogSpout implements ITridentSpout<Long> {
	private static final Logger LOG = LoggerFactory.getLogger(LogSpout.class);
	private static final long serialVersionUID = 1L;

	@Override
	public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
		LOG.info("LogSpout.getCoordinator({}, conf, context)", txStateId);
		return new LogBatchCoordinator();
	}

	@Override
	public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
		LOG.info("LogSpout.getEmitter({}, conf, context)", txStateId);
		return new LogEmitter();
	}

	@Override
	public Map getComponentConfiguration() {
		LOG.info("LogSpout.getComponentConfiguration()");
		return null;
	}

	@Override
	public Fields getOutputFields() {
		LOG.info("LogSpout.getOutputFields()");
		return new Fields("logString");
	}
}
