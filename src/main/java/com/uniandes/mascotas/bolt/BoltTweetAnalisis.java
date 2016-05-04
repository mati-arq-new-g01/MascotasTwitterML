package com.uniandes.mascotas.bolt;

import java.util.Map;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

public class BoltTweetAnalisis extends BaseRichBolt {

	private OutputCollector collector = null;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		final Status tweet = (Status) tuple.getValueByField("status");
                String interes="";
		String tuit = tweet.getText();
                String usuario = tweet.getUser().getScreenName();
                if(tuit.indexOf("perro")>=0 || tuit.indexOf("perrito")>=0 || tuit.indexOf("cachorro")>=0 || tuit.indexOf("cachorrito")>=0){
                    interes="perro";
                } else if(tuit.indexOf("gato")>=0 || tuit.indexOf("gatito")>=0 || tuit.indexOf("minino")>=0 || tuit.indexOf("gatico")>=0){
                    interes = "gato";
                } else if(tuit.indexOf("animal")>=0 || tuit.indexOf("mascota")>=0 || tuit.indexOf("mejor amigo del hombre")>=0 || tuit.indexOf("adopta")>=0){
                    interes = "indiferente";
                } else{
                    interes = "";
                }
                //final String[] words = tweet.getText().split(" ");
                
                //////
                //if(tweet.getPlace()!=null){
                //System.out.println("TWEET ===========>  " + tweet.getPlace().getCountry());
                
                /*Mongo mongo = new MongoClient("localhost",27017);
                DB db;
                DBCollection tabla;
                db= mongo.getDB("mascotas");
                tabla = db.getCollection("tweets");

                BasicDBObject document =  new BasicDBObject();
                document.put("tweet", "'" + tweet.getText() + "'");
                document.put("usuario", "'" + tweet.getUser().getScreenName() + "'");
               // document.put("pais", tweet.getPlace().getCountry());
                tabla.insert(document);*/
                System.out.println("JHLC: " + tweet.getText());
                
                //}
                ///////

		//for (String word : words) {
		//	collector.emit(new Values(word));
		//}
                collector.emit(new Values(interes));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("interes"));
	}
}
