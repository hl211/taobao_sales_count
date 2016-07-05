package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable
{
    
    // 这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_teamcode_";
    
    public static String prex_taobao = "platformTaobao_teamcode_";
    
    public static String prex_ratio = "ratio_teamcode_";
    
    public static String MyNameService = "192.168.1.211:9876";
    
    // 这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "hl_love_p";
    
    public static String MetaConsumerGroup = "my_hl";
    
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    
    public static String TairConfigServer = "10.101.72.127:5198";
    
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    
    public static String TairGroup = "group_tianchi";
    
    public static Integer TairNamespace = 1;
}
