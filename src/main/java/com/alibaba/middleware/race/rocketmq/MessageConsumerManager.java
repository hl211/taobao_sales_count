package com.alibaba.middleware.race.rocketmq;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RocketMQConfig;
import com.alibaba.middleware.race.internal.tools.FastBeanUtils;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class MessageConsumerManager
{
    
    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerManager.class);
    
    MessageConsumerManager()
    {
    }
    
    public static MQConsumer getConsumerInstance(RocketMQConfig config, MessageListener listener, Boolean isPushlet)
        throws MQClientException
    {
        LOG.info("Begin to init consumer,instanceName->{},configuration->{}", new Object[] {config.getInstanceName(),
            config});
        
        if (BooleanUtils.isTrue(isPushlet))
        {
            // DefaultMQPushConsumer pushConsumer =
            // (DefaultMQPushConsumer)FastBeanUtils.copyProperties(config, DefaultMQPushConsumer.class);
            
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(config.getInstanceName());
            // pushConsumer.setConsumerGroup(config.getGroupId());
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            
            // 订阅topic
            pushConsumer.subscribe(config.getTopic(), "*");
            
            // 自己加的
            // pushConsumer.setNamesrvAddr(RaceConfig.MqNameService);
            
            if (listener instanceof MessageListenerConcurrently)
            {
                pushConsumer.registerMessageListener((MessageListenerConcurrently)listener);
            }
            if (listener instanceof MessageListenerOrderly)
            {
                pushConsumer.registerMessageListener((MessageListenerOrderly)listener);
            }
            return pushConsumer;
        }
        else
        {
            DefaultMQPullConsumer pullConsumer =
                (DefaultMQPullConsumer)FastBeanUtils.copyProperties(config, DefaultMQPullConsumer.class);
            pullConsumer.setConsumerGroup(config.getGroupId());
            
            return pullConsumer;
        }
    }
}