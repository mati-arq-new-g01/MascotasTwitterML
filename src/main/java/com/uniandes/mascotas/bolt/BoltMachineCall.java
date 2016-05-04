package com.uniandes.mascotas.bolt;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.elibom.client.ElibomRestClient;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

public class BoltMachineCall extends BaseRichBolt {

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public void execute(Tuple tuple) {
		final String interes = tuple.getStringByField("interes");
		final String datos = tuple.getStringByField("datos");
		
		try{
                    
                    
                    
                    
                    ////bloque mongo
                    Mongo mongo = new MongoClient("localhost",27017);
                DB db;
                DBCollection tabla;
                db= mongo.getDB("mascotas");
                tabla = db.getCollection("Adopcion");

                BasicDBObject document =  new BasicDBObject();
                
                
                //Aca va el llamado a la ML. Si el resultado es afirmativo se ejecutan las siguientes instrucciones.
                
                if(!interes.isEmpty()){    
                document.put("cuenta", "'@jhlopez86'");    
                document.put("interes", "'" + interes + "'");
                document.put("datos", "'" + datos + "'");
                    
                    tabla.insert(document);
                
                
                //aca enviamos el sms
                    
                ElibomRestClient elibom = new ElibomRestClient("jairo8005@hotmail.com", "LTC6RNXF57");
                String deliveryId = elibom.sendMessage("573004183070", "adopta un lindo " + interes + " en la universidad de los Alpes");
                System.out.println("deliveryId: " + deliveryId);
                System.out.println(" SMS enviado OK!");
                    
                }
                
                    
                  }catch(Exception e){
                   // System.out.println("ERROR");      
                  }
                
                
                
                
                
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
