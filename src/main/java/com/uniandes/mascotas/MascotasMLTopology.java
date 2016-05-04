package com.uniandes.mascotas;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.uniandes.mascotas.bolt.BoltMachineCall;
import com.uniandes.mascotas.bolt.BoltTweetAnalisis;
import com.uniandes.mascotas.bolt.BoltDataBuild;
import com.uniandes.mascotas.spout.SpoutTweetsStreamingFilter;

public class MascotasMLTopology {

	public static void main(String... args) throws AlreadyAliveException, InvalidTopologyException {
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterSpout", new SpoutTweetsStreamingFilter());
		builder.setBolt("tweetSplitterBolt", new BoltTweetAnalisis(), 2).shuffleGrouping("twitterSpout");
		builder.setBolt("wordCounterBolt", new BoltDataBuild(),2).fieldsGrouping("tweetSplitterBolt", new Fields("interes"));
		builder.setBolt("countPrinterBolt", new BoltMachineCall(), 2).fieldsGrouping("wordCounterBolt", new Fields("interes"));

		final Config conf = new Config();
		conf.setDebug(false);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("wordCountTopology", conf, builder.createTopology());
	}
}
