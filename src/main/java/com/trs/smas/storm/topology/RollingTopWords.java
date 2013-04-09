package com.trs.smas.storm.topology;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.trs.smas.storm.bolt.IntermediateRankingsBolt;
import com.trs.smas.storm.bolt.RollingCountBolt;
import com.trs.smas.storm.bolt.SegmentBolt;
import com.trs.smas.storm.bolt.TotalRankingsBolt;
import com.trs.smas.storm.spout.RandomSentenceSpout;
import com.trs.smas.storm.tools.Rankable;
import com.trs.smas.storm.tools.Rankings;
import com.trs.smas.storm.util.StormRunner;

public class RollingTopWords {

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
    private static final int TOP_N = 5;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public RollingTopWords() throws InterruptedException {
        builder = new TopologyBuilder();
        topologyName = "slidingWindowCounts";
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }
    
    static class Print extends BaseRichBolt{

    	private static final long serialVersionUID = -6666299516928475115L;

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
				OutputCollector collector) {
		}

		@Override
		public void execute(Tuple input) {
			System.out.println("\n\n\n\n==================================================");
			Rankings rankings = (Rankings)input.getValue(0);
			for(Rankable rankable : rankings.getRankings()){
				System.out.println(rankable.getObject()+ ":"+rankable.getCount());
			}
			System.out.println("==================================================\n\n\n\n");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}}

    private void wireTopology() throws InterruptedException {
        String spoutId = "sentenceGenerator";
        String segmentId = "segment";
        String counterId = "counter";
        String intermediateRankerId = "intermediateRanker";
        String totalRankerId = "finalRanker";
//        builder.setSpout(spoutId, new TestWordSpout(), 5);
        builder.setSpout(spoutId, new RandomSentenceSpout(100), 2);
        builder.setBolt(segmentId, new SegmentBolt(),1).shuffleGrouping(spoutId);
        builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).fieldsGrouping(segmentId, new Fields("word"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId,
            new Fields("obj"));
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
        builder.setBolt("print", new Print()).globalGrouping(totalRankerId);
    }

    public void run() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public static void main(String[] args) throws Exception {
        new RollingTopWords().run();
    }
}
