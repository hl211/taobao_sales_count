package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RocketMQConfig;

public class MainTopology
{
    private static Logger LOG = LoggerFactory.getLogger(MainTopology.class);
    
    public static void main(String[] args)
        throws Exception
    {
        
        Config config = new Config();
        int spout_Parallelism_hint = 2;
        int split_Parallelism_hint = 6;
        int del_Parallelism_hint = 1;
        int count_Parallelism_hint = 1;
        
        // paymessage
        TopologyBuilder builder = new TopologyBuilder();
        
        RocketMQConfig payConig = new RocketMQConfig();
        payConig.setInstanceName("pay");
        payConig.setTopic(RaceConfig.MqPayTopic);
        MessageSpout paySpout = new MessageSpout();
        paySpout.setConfig(payConig);
        
        builder.setSpout("payspout", paySpout, spout_Parallelism_hint);
        
        RocketMQConfig taobaoConig = new RocketMQConfig();
        taobaoConig.setInstanceName("taobao");
        taobaoConig.setTopic(RaceConfig.MqTaobaoTradeTopic);
        MessageSpout taoBaoSpout = new MessageSpout();
        taoBaoSpout.setConfig(taobaoConig);
        
        builder.setSpout("taobaospout", taoBaoSpout, spout_Parallelism_hint);
        
        RocketMQConfig tmallConig = new RocketMQConfig();
        tmallConig.setInstanceName("tmall");
        tmallConig.setTopic(RaceConfig.MqTmallTradeTopic);
        MessageSpout tmallSpout = new MessageSpout();
        tmallSpout.setConfig(tmallConig);
        
        builder.setSpout("tmallspout", tmallSpout, spout_Parallelism_hint);
        
        builder.setBolt("splitbolt", new SplitMessageBolt(), split_Parallelism_hint)
            .shuffleGrouping("payspout")
            .shuffleGrouping("taobaospout")
            .shuffleGrouping("tmallspout");
        
        // pay
        builder.setBolt("paybolt", new PayBolt(), del_Parallelism_hint).shuffleGrouping("splitbolt",
            RaceConfig.MqPayTopic);
        builder.setBolt("paycountbolt", new PayCountBolt(), count_Parallelism_hint).fieldsGrouping("paybolt",
            new Fields("time", "type"));
        
        // taobao
        builder.setBolt("taobaobolt", new TaoBaoBolt(), del_Parallelism_hint).shuffleGrouping("splitbolt",
            RaceConfig.MqTaobaoTradeTopic);
        builder.setBolt("taobaocountbolt", new TaoBaoCountOrderBolt(), count_Parallelism_hint)
            .fieldsGrouping("taobaobolt", new Fields("time"));
        
        // tmall
        builder.setBolt("tmallbolt", new TmallBolt(), del_Parallelism_hint).shuffleGrouping("splitbolt",
            RaceConfig.MqTmallTradeTopic);
        builder.setBolt("tmallcountbolt", new TmallCountOrderBolt(), count_Parallelism_hint)
            .fieldsGrouping("tmallbolt", new Fields("time"));
        
        String topologyName = RaceConfig.JstormTopologyName;
        // config.put("TIME_OFFSET", 10);
        try
        {
            LocalCluster cluster = new LocalCluster();
            
            // config.put(Config.STORM_CLUSTER_MODE, "local");
            config.setDebug(false);
            cluster.submitTopology(topologyName, config, builder.createTopology());
            
            Thread.sleep(5000000);
            
            cluster.shutdown();
            
            // StormSubmitter.submitTopology(topologyName, conf,
            // builder.createTopology());
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
