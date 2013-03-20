package com.trs.smas.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 2749390516622269088L;
	
	SpoutOutputCollector _collector;
    Random _rand;    
    long tupleInterval = 100;
    
    public RandomSentenceSpout(long tupleInterval){
    	 if (tupleInterval < 0) {
             throw new IllegalArgumentException("tupleInterval must be greater than or equals zero (you requested "
                 + tupleInterval + ")");
         }
    	this.tupleInterval = tupleInterval;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(tupleInterval);
        String[] sentences = new String[] {
            "史上最恐怖课表！】网友@米琦要努力向上 说：星期一殡葬概论、收尸、整容；星期二烧骨、挽联、传染病…课这么满，怎么活。网友惊呼“好恐怖！”这份课表是长沙民政学院现代殡葬管理专业，学生说：其实课程并不“灵异恐怖”，就业前景也不错，只不过…对…象…比…较…难…找！",
            "【400多只大天鹅来京】#聚焦北京#“第一批候鸟已经抵达野鸭湖‘报到’了”，不久前，黑豹野生动物保护站在巡护监测候鸟迁徙情况时，发现了400多只天鹅的踪影。此次天鹅从南方迁徙北上的时间比往年提前了半个月，可能与北方冰化时间较早有关。估计10天后，野鸭湖湿地将迎来天鹅到京的高峰。",
            "我是一个过程正义的追求者，我始终认为，如果不考虑过程，为了一个所谓理想目的，就可以不择手段。好比同追求破案，采取的刑讯逼供一样。虽然历史上很多弱势群体为了追求利益，采取一些非正常手段，但是社会在发展，我们不是非得去重复历史。何况非正义手段会给无辜人士带来惨痛的伤害！",
            "#好女孩有梦想# 今天的书送给80后妈妈@寒雪细语 她说她到现在还不会做饭，自己的老妈又要帮着带孩子又要做饭，自己做了妈妈才体会到可怜天下父母心，为了让老妈轻松点，今年的愿望是好好学做饭。加油~！做饭又不是啥高科技，失败几次自然就会啦！请将姓名地址电话私信给厨房君^_^",
            "【动作脚本大观】什么是动作脚本?想了解在Actionscript3.0环境下Flash的编程操作吗?本课程主要讲述了Flash(Actionscript3.0)中如何通过函数的创建、调用以及对事件对象参数的巧妙应用实现为对象指派事件，帮助你完成动画制作。《设计教程——flash》第40讲一些详细的操作手法。"};
        String sentence = sentences[_rand.nextInt(sentences.length)];
        _collector.emit(new Values(sentence));
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
    
}