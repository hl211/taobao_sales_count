/**   
 * Copyright © 2016 公司名. All rights reserved.
 * 
 * @Title: PayBolt.java 
 * @Prject: preliminary.demo
 * @Package: com.alibaba.middleware.race.jstorm 
 * @Description: TODO
 * @author: hl   
 * @date: 2016年7月2日 上午8:46:43 
 * @version: V1.0   
 */
package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;

/**
 * @ClassName: PayBolt
 * @Description: TODO
 * @author: hl
 * @date: 2016年7月2日 上午8:46:43
 */
public class TaoBaoBolt implements IRichBolt
{
    private static final long serialVersionUID = 7591260982890048043L;
    
    private static final Logger LOG = LoggerFactory.getLogger(TaoBaoBolt.class);
    
    private OutputCollector collector;
    
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
        
    }
    
    @Override
    public void execute(Tuple input)
    {
        String type = (String)input.getStringByField("type");
        OrderMessage orderMessage = (OrderMessage)input.getValueByField("message");
        
        try
        {
            if (type.equals(RaceConfig.MqTaobaoTradeTopic))
            {
                
                collector.emit(new Values(RaceConfig.MqTaobaoTradeTopic, isMin(orderMessage.getCreateTime()),
                    orderMessage.getTotalPrice()));
            }
        }
        catch (Exception e)
        {
            collector.fail(input);
            return;
            // throw new FailedException(e);
        }
        collector.ack(input);
    }
    
    @Override
    public void cleanup()
    {
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        Fields fields = new Fields("type", "time", "money");
        declarer.declare(fields);
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
    
    public long isMin(long time)
    {
        long temp;
        temp = time - time % 60000;
        return temp / 1000;
        
    }
    
}
