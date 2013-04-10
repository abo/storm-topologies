package com.trs.smas.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class TridentLogStat {

	public static class Type extends BaseFunction {

		private static final long serialVersionUID = -5480605206801270360L;
		
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String log = tuple.getString(0).substring(1,tuple.getString(0).length()-2);
			String [] components = log.split("] [");
			String time = components[0].substring(0,components[0].length()-3);
 			collector.emit(new Values(time+" - "+components[1]));
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("log"), 3,
				new Values("[2013-04-10 13:55:04,109] [query] [builder] [nocached] [228] [view@192.9.100.23:8885/sina_status_20130401,view@192.9.100.23:8885/sina_status_20130501] [view@192.9.100.23:8885/sina_status_20130401;sina_status_20130501] [219.142.104.66] [212] [-IR_CREATED_AT] [15] [(IR_CREATED_DATE=(2013.03.12 to 2013.04.10))*(IR_SCREEN_NAME=(百嘉购物商场))*((((珠海+佛山+江门+中山+东莞+湛江+茂名+阳江+肇庆+云浮+汕头+潮州+揭阳+汕尾+惠州+河源+梅州+韶关+清远+深圳+广州+广东)and/sen(环保+环境保护))and/sen(滥用职权+玩忽职守+失职+渎职+违纪+吃空饷+贿赂+行贿+索贿+受贿+贪污+失察+失误+落马+双规+腐败+庇护伞+官商勾结+包庇+纵容+黑社会+恶势力+黑老大+小金库+检举+揭发+下台+停职+免职+公款+变相敛财+包养+不检点+公款旅游+公款吃喝+吃空饷+小金库+以权谋私+三公经费+包养情妇+养二奶+养小三+艳照+不雅视频+生活糜烂+卷款外逃+境外资产+转移资产+国外定居+子女国外留学+名牌香烟+名表+巨额财产+名牌手表+玩3P+跑官卖官+违规提拔+裸官+逃官+玩3p+打人+学历造假+肇事+违规采购+贪官+下马)))*((((珠海+佛山+江门+中山+东莞+湛江+茂名+阳江+肇庆+云浮+汕头+潮州+揭阳+汕尾+惠州+河源+梅州+韶关+清远+深圳+广州+广东)and/sen(环保+环境保护))and/sen(滥用职权+玩忽职守+失职+渎职+违纪+吃空饷+贿赂+行贿+索贿+受贿+贪污+失察+失误+落马+双规+腐败+庇护伞+官商勾结+包庇+纵容+黑社会+恶势力+黑老大+小金库+检举+揭发+下台+停职+免职+公款+变相敛财+包养+不检点+公款旅游+公款吃喝+吃空饷+小金库+以权谋私+三公经费+包养情妇+养二奶+养小三+艳照+不雅视频+生活糜烂+卷款外逃+境外资产+转移资产+国外定居+子女外留学+名牌香烟+名表+巨额财产+名牌手表+玩3P+跑官卖官+违规提拔+裸官+逃官+玩3p+打人+学历造假+肇事+违规采购+贪官+下马)))] [<com.trs.smas.model.search.impl.TRSSearchServiceImpl.trace<com.trs.smas.model.search.impl.TRSSearchServiceImpl.pageList<com.trs.smas.model.microblog.impl.StatusSearchServiceImpl.pageList<com.trs.smas.model.system.widget.WeiboStatAnalysisWidget.getSectionList<com.trs.smas.model.system.widget.AbstractWidget.getSectionList<com.trs.smas.ui.controller.organization.analy.WidgetController.sectionList]"), 
				new Values("[2013-04-10 13:59:42,469] [query] [native] [nocached] [946] [system@192.168.201.190:8885/smas_additional,system@192.168.201.190:8885/news_history,system@192.168.201.190:8885/news_201203,system@192.168.201.190:8885/news_201204,system@192.168.201.190:8885/news_201205,system@192.168.201.190:8885/news_201206,system@192.168.201.190:8885/news_201207,system@192.168.201.190:8885/news_201208,system@192.168.201.190:8885/news_201209,system@192.168.201.190:8885/news_201210,system@192.168.201.190:8885/smas_cps_hot_news,system@192.168.201.190:8885/smas_hotword,system@192.168.201.190:8885/news_201211] [system@192.168.201.190:8885/smas_additional;news_history;news_201203;news_201204;news_201205;news_201206;news_201207;news_201208;news_201209;news_201210;smas_cps_hot_news;smas_hotword;news_201211] [123.114.42.98] [91] [null] [1] [(IR_BBSNUM=(0,1))*(MD5TAG=(\b48591\a3\b1\d\f\b371))*(ir_groupname=国内新闻% and ir_sitename!=(%贴吧+%论坛+%社区) and ir_urltitle=(凯美瑞) and ir_urldate=(2012.1.1 to 2012.11.30) and  ir_urltitle!=(行情+报价+转让+供应+股市+A股+B股+H股+沪深+股票+证券+个股+上市公司重大事项公告+招募+季报+年报+上证综指+深证成指+券商+答疑+广告+分类信息))] [<com.trs.smas.model.search.impl.TRSSearchServiceImpl.trace<com.trs.smas.model.search.impl.TRSSearchServiceImpl.pageList<com.trs.smas.model.document.impl.DocumentSearchServiceImpl.findFirstDocument<com.trs.smas.model.system.widget.FocusArticleListWidget.widgetData<com.trs.smas.model.system.widget.FocusArticleListWidget.getWidgetData<com.trs.smas.model.system.widget.AbstractWidget.createWidget<com.trs.smas.ui.controller.organization.analy.WidgetController.getWidget]"),
				new Values("[2013-04-10 13:59:42,380] [count] [builder] [nocached] [1240] [system@192.168.201.190:8885/smas_additional,system@192.168.201.190:8885/news_history,system@192.168.201.190:8885/news_201203,system@192.168.201.190:8885/news_201204,system@192.168.201.190:8885/news_201205,system@192.168.201.190:8885/news_201206,system@192.168.201.190:8885/news_201207,system@192.168.201.190:8885/news_201208,system@192.168.201.190:8885/news_201209,system@192.168.201.190:8885/news_201210,system@192.168.201.190:8885/smas_cps_hot_news,system@192.168.201.190:8885/smas_hotword,system@192.168.201.190:8885/news_201211] [system@192.168.201.190:8885/smas_additional;news_history;news_201203;news_201204;news_201205;news_201206;news_201207;news_201208;news_201209;news_201210;smas_cps_hot_news;smas_hotword;news_201211] [123.114.42.98] [91] [] [0] [(ir_groupname=国内新闻% and ir_sitename!=(%贴吧+%论坛+%社区) and ir_urltitle, ir_content+=(客车+巴士+大巴+中巴+公交车) and ir_urldate=(2012.1.1 to 2012.11.30) and  ir_urltitle!=(行情+报价+转让+供应+股市+A股+B股+H股+沪深+股票+证券+个股+上市公司重大事项公告+招募+季报+年报+上证综指+深证成指+券商+答疑+广告+分类信息))*(ir_urltitle, ir_content+=((常隆)and/sen(客车)))] [<com.trs.smas.model.search.impl.TRSSearchServiceImpl.trace<com.trs.smas.model.search.impl.TRSSearchServiceImpl.count<com.trs.smas.model.document.impl.DocumentSearchServiceImpl.count<com.trs.smas.model.system.widget.StatAnalysisWidget.statisticByCategory<com.trs.smas.model.system.widget.StatAnalysisWidget.getWidgetData<com.trs.smas.model.system.widget.AbstractWidget.createWidget<com.trs.smas.ui.controller.organization.analy.WidgetController.getWidget]"),
				new Values("[2013-04-10 13:59:42,543] [count] [builder] [nocached] [642] [system@192.168.201.190:8885/smas_additional,system@192.168.201.190:8885/news_201301,system@192.168.201.190:8885/smas_cps_hot_news,system@192.168.201.190:8885/news_201302,system@192.168.201.190:8885/smas_hotword] [system@192.168.201.190:8885/smas_additional;news_201301;smas_cps_hot_news;news_201302;smas_hotword] [123.114.42.98] [91] [] [0] [(ir_groupname=国内新闻 and ir_urltitle, ir_content+=(汽车+乘用车+车型+车公司+车有限公司+车企) and ir_urldate=(2013.01.01 to 2013.02.25) and ir_urltitle!=(行情+报价+转让+供应+股市+A股+B股+H股+沪深+股票+券+个股+上市公司重大事项公告+招募+季报+年报+上证综指+深证成指+券商+答疑+广告+分类信息))*(ir_urltitle, ir_content+=东风裕隆)] [<com.trs.smas.model.search.impl.TRSSearchServiceImpl.trace<com.trs.smas.model.search.impl.TRSSearchServiceImpl.count<com.trs.smas.model.document.impl.DocumentSearchServiceImpl.count<com.trs.smas.model.system.widget.StatAnalysisWidget.statisticByCategory<com.trs.smas.model.system.widget.StatAnalysisWidget.getWidgetData<com.trs.smas.model.system.widget.StatAnalysisWidget.getWidgetDataForAPI<com.trs.smas.model.system.widget.AbstractWidget.createWidget<com.trs.smas.ui.controller.organization.analy.WidgetController.getWidget]"), 
				new Values("[2013-04-10 13:59:41,173] [count] [builder] [nocached] [2619] [system@192.168.201.190:8885/smas_additional,system@192.168.201.190:8885/smas_cps_hot_news,system@192.168.201.190:8885/news_201303,system@192.168.201.190:8885/smas_hotword] [system@192.168.201.190:8885/smas_additional;smas_cps_hot_news;news_201303;smas_hotword] [123.114.42.98] [91] [] [0] [(ir_groupname=国内新闻%  and ir_sitename!=(%贴吧+%坛+%社区) and ir_urltitle, ir_content+=(汽车+车型+商用车+货车+重卡+重型车+牵引车+中卡+轻卡+客车+公交车+巴士+大巴+中巴+轻客+校车+车公司+车有限公司) and ir_urldate=(2013.3.1 to 2013.3.31) and ir_urltitle!=(行情+报价+转让+供应+股市+A股+B股+H股+沪深+股票+证券+个股+上市公司重大事项公告+招募+季报+年报+上证综指+深证成指+券商+答疑+广告+分类信息))*(ir_urltitle=(上依红+上汽依维柯红岩))] [<com.trs.smas.model.search.impl.TRSSearchServiceImpl.trace<com.trs.smas.model.search.impl.TRSSearchServiceImpl.count<com.trs.smas.model.document.impl.DocumentSearchServiceImpl.count<com.trs.smas.model.system.widget.StatAnalysisWidget.statisticByCategory<com.trs.smas.model.system.widget.StatAnalysisWidget.getWidgetData<com.trs.smas.model.system.widget.AbstractWidget.createWidget<com.trs.smas.ui.controller.organization.analy.WidgetController.getWidget]"));
		spout.setCycle(true);

		TridentTopology topology = new TridentTopology();
		TridentState wordCounts = topology
				.newStream("spout1", spout)
				.parallelismHint(16)
				.each(new Fields("log"), new Type(), new Fields("type"))
				.groupBy(new Fields("type"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(),
						new Fields("count")).parallelismHint(16);

		topology.newDRPCStream("words", drpc)
				.each(new Fields("args"), /*new Split()*/null, new Fields("word"))
				.groupBy(new Fields("word"))
				.stateQuery(wordCounts, new Fields("word"), new MapGet(),
						new Fields("count"))
				.each(new Fields("count"), new FilterNull())
				.aggregate(new Fields("count"), new Sum(), new Fields("sum"));
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
			for (int i = 0; i < 100; i++) {
				System.out.println("DRPC RESULT: "
						+ drpc.execute("words", "cat the dog jumped"));
				Thread.sleep(1000);
			}
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
		}
	}
}
