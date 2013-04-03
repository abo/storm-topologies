package com.trs.smas.storm.spout;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.I0Itec.zkclient.ZkClient;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class KafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1766861564554924183L;

	private static final Charset CHARSET = Charset.forName("UTF-8");

	SpoutOutputCollector collector;
	ConsumerConnector kafkaConsumerConnector;
	ConsumerIterator<Message> kafkaMessageIterator;
	Properties kafkaConsumerProps;
	String kafkaTopic;

	public KafkaSpout(Properties consumerProps, String topic) {
		this.kafkaTopic = topic;
		this.kafkaConsumerProps = new Properties();
		Iterator<String> iter = consumerProps.stringPropertyNames().iterator();
		while (iter.hasNext()) {
			String k = iter.next();
			this.kafkaConsumerProps.put(k, consumerProps.get(k));
		}

		if ("smallest".equals(kafkaConsumerProps.getProperty("autooffset.reset"))) {// from-beginning
			tryCleanupZookeeper(kafkaConsumerProps.getProperty("zk.connect"),
					kafkaConsumerProps.getProperty("groupid"));
		}
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		kafkaConsumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				kafkaConsumerProps));

		kafkaMessageIterator = kafkaConsumerConnector
				.createMessageStreamsByFilter(new Whitelist(this.kafkaTopic)).get(0)
				.iterator();
	}

	@Override
	public void nextTuple() {
		if (this.kafkaMessageIterator.hasNext()) {
			MessageAndMetadata<Message> messageAndTopic = this.kafkaMessageIterator.next();
			ByteBuffer payload = messageAndTopic.message().payload();
			this.collector.emit(new Values(messageAndTopic.topic(), new String(
					payload.array(), payload.arrayOffset(), payload.limit(),
					CHARSET)));
		} else {
			Utils.sleep(100);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "entry"));
	}

	@Override
	public void close() {
		kafkaConsumerConnector.shutdown();
		tryCleanupZookeeper(kafkaConsumerProps.getProperty("zk.connect"),
				kafkaConsumerProps.getProperty("groupid"));
		super.close();
	}

	public static void tryCleanupZookeeper(String zkUrl, String groupId) {
		try {
			String dir = "/consumers/" + groupId;
			System.out.println("Cleaning up temporary zookeeper data under "
					+ dir + ".");
			ZkClient zk = new ZkClient(zkUrl, 30 * 1000, 30 * 1000);
			zk.deleteRecursive(dir);
			zk.close();
		} catch (Exception e) {
		}
	}

}
