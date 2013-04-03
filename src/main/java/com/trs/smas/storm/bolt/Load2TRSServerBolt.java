package com.trs.smas.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.trs.client.TRSConnection;
import com.trs.client.TRSException;

public class Load2TRSServerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -7489261520275127899L;
	
	private static final String [] FIELD_NAMES = new String []{
		"bid"/*:message bid*/,
		"uid"/*:message user uid*/,
		"username"/*:user name*/,
		"v_class"/*: "V" or "" means VIP or NotVIP*/,
		"content"/*:message content*/,
		"img"/*: image url if included*/,
		"time"/*: message post time*/,
		"source"/*:device used to post the message*/,
		"rt_num"/*: # retweet times*/,
		"cm_num"/*: # comment times*/,
		"rt_uid"/*: if this tweet is a retweet, this is the retweet user uid*/,
		"rt_username"/*: retweet user name*/,
		"rt_v_class"/*: retweet V_class*/,
		"rt_content"/*: retweet content*/,
		"rt_img"/*: image url if include any*/,
		"src_rt_num"/*: source tweet # retweet times*/,
		"src_cm_num"/*:source tweet # comment times*/,
		"gender"/*: "f" or “m” means female or male*/,
		"rt_bid"/*: retweet message bid*/,
		"location"/*:location code of this post`s author*/,
		"rt_mid"/*:mid of retweet*/,
		"mid"/*:mid of this post*/,
		"lat"/*:the latitude of this post source*/,
		"lon"/*:the longitude of this post source*/,
		"lbs_type"/*:the type of this location based acitivty*/,
		"lbs_title"/*:the name of this position*/,
		"poiid"/*:the position id*/,
		"links"/*:url links extracted from this post content,comma separated.exp., "http://t.cn/12,http://t.cn/dada"*/,
		"hashtags"/*:hashtags extracted from this post content,comma separated.exp.,"#a#,#china#"*/,
		"ats"/*:the mentioned usernames extracted from this post content,comma separated.exp.,“ (张三,)李四"*/,
		"rt_links"/*:url links extracted from retweet content,comma separated. exp.,"http://t.cn/12,http://t.cn/dada"*/,
		"rt_hashtags"/*:hashtags extracted from retweet content,comma separated. exp.,"#a#,#china#"*/,
		"rt_ats"/*:the mentioned usernames extracted from retweet content，comma separated。exp.,"(张三,)李四"*/,
		"v_url"/*:if the author of this post is verified  by sina, this is the image url of verified type.*/,
		"rt_v_url"/*:if the author of retweet is verified  by sina, this is the image url of verified type.*/,
		"u_type"};
	
	private String host;
	private String port;
	private String username;
	private String password;
	private String database;
	
	TRSConnection connection;

	public Load2TRSServerBolt(String host,String port,String username,String password,String database) throws TRSException{
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.database = database;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String [] fieldValues = (String []) input.getValue(1);
		try {
			if(connection == null){
				connection = new TRSConnection();
			}
			if(!connection.isValid()){
				connection.connect(host, port, username, password);
			}
			StringBuilder sb = new StringBuilder();
			for(int i = 0 ; i < FIELD_NAMES.length ; i ++ ){
				sb.append(FIELD_NAMES[i]).append("=").append(fieldValues[i]).append("\n");
			}
			connection.executeInsert(database, username, sb.toString());
		} catch (TRSException e) {
			collector.reportError(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		if(connection != null){
			connection.close();
		}
		super.cleanup();
	}

	
	abstract public class DatabaseSelector{
		public abstract String select(Tuple input);
	}
}
