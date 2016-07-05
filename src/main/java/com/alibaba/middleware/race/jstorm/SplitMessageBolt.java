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
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

public class SplitMessageBolt implements IRichBolt
{
    private static final long serialVersionUID = 7591260982890048043L;
    
    private static final Logger LOG = LoggerFactory.getLogger(SplitMessageBolt.class);
    
    private OutputCollector collector;
    
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple input)
    {
        MessageExt msg = (MessageExt)input.getValueByField("message");
        
        try
        {
            byte[] body = msg.getBody();
            String topic = msg.getTopic();
            if (topic.equals(RaceConfig.MqPayTopic))
            {
                PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                
                collector.emit(RaceConfig.MqPayTopic, new Values(RaceConfig.MqPayTopic, paymentMessage));
                
            }
            
            if (topic.equals(RaceConfig.MqTaobaoTradeTopic))
            {
                OrderMessage ordermessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                collector.emit(RaceConfig.MqTaobaoTradeTopic, new Values(RaceConfig.MqTaobaoTradeTopic, ordermessage));
            }
            if (topic.equals(RaceConfig.MqTmallTradeTopic))
            {
                OrderMessage ordermessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                collector.emit(RaceConfig.MqTmallTradeTopic, new Values(RaceConfig.MqTmallTradeTopic, ordermessage));
            }
            // LOG.info("Messages:" + msgObj + "\n statistics:");
            collector.ack(input);
            
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
        // Fields fields = new Fields("type", "message");
        // declarer.declare(fields);
        declarer.declareStream(RaceConfig.MqPayTopic, new Fields("type", "message"));
        declarer.declareStream(RaceConfig.MqTaobaoTradeTopic, new Fields("type", "message"));
        declarer.declareStream(RaceConfig.MqTmallTradeTopic, new Fields("type", "message"));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
    
}
