package com.trs.smas.storm.bolt;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.trs.smas.storm.util.DateUtil;
import com.trs.smas.storm.util.StringUtil;

/**
 * 解析CSV格式的行
 * @author huangshengbo
 *
 */
public class LineParseBolt extends BaseBasicBolt {
	
	private static final Logger LOG = Logger.getLogger(LineParseBolt.class);

	private static final long serialVersionUID = 1703468659718068776L;
	
	public LineParseBolt(){
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//"bid","IR_UID","IR_SCREEN_NAME","v_class","IR_STATUS_CONTENT","IR_THUMBNAIL_PIC","IR_CREATED_AT","IR_VIA","IR_RTTCOUNT","IR_COMMTCOUNT","IR_RETWEETED_UID","IR_RETWEETED_SCREEN_NAME","rt_v_class","rt_content","rt_img","src_rt_num","src_cm_num","gender","rt_bid","location","IR_RETWEETED_MID","IR_MID","lat","lon","lbs_type","lbs_title","poiid","links","hashtags","ats","rt_links","rt_hashtags","rt_ats","v_url","rt_v_url","u_type"
		String line = input.getString(1);
		if(line == null || line.trim().length() == 0){
			return;
		}
		String [] fields = StringUtil.parse(input.getString(1));
		String [] finalFields = new String[50];
		for(int i = 0 ; i < fields.length ; i ++){
			finalFields[i] = fields[i];
		}
		finalFields[35] = ""; //u_type
		finalFields[36] = "http://weibo.com/"+ finalFields[1]+"/"+finalFields[0];//"IR_URLNAME",
		finalFields[37] = DateUtil.formatForTRSServer(System.currentTimeMillis());//"IR_LASTTIME",
		finalFields[38] = "http://weibo.com/"+ finalFields[10]+"/"+finalFields[18]; //"IR_RETWEETED_URL",
		try{
			finalFields[39] = finalFields[6].substring(0, "yyyy-MM-dd".length()).replace('-', '.'); //"IR_CREATED_DATE",
			finalFields[40] = finalFields[6].substring(0, "yyyy".length());//"IR_CREATED_YEAR",
			finalFields[41] = finalFields[6].substring(0, "yyyy-MM".length()).replace('-', '.');//"IR_CREATED_MONTH",
			finalFields[42] = finalFields[6].substring("yyyy-MM-dd ".length(), "yyyy-MM-dd hh".length());//"IR_CREATED_HOUR",
			finalFields[43] = "$time";//"IR_LOADTIME"
			//,"IR_SID","IR_HKEY","IR_GROUPNAME","IR_SITENAME","IR_STATUS_BODY","IR_CHANNEL"
			collector.emit(new Values(input.getString(0),finalFields));
		}catch(Exception e){
			LOG.error("parse["+input.getString(1)+"] failed,array: \n"+StringUtil.join(fields),e);
			collector.reportError(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic","fields"));
	}
}
