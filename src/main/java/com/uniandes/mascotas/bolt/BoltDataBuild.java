package com.uniandes.mascotas.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltDataBuild extends BaseRichBolt {

	private OutputCollector collector = null;
	//private Map<String, MutableInt> words = null;
        private String interes=null;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		//this.words = new HashMap<String, MutableInt>();
                this.interes="";
	}

	@Override
	public void execute(Tuple input) {
		final String word = input.getStringByField("interes");
		

		interes=word;
		collector.emit(new Values(interes,"femenino,bogota,soltero,0,bachillerato,2"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("interes", "datos"));
	}
}
