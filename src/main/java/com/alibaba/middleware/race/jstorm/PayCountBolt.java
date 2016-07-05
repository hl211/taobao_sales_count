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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

/**
 * @ClassName: PayBolt
 * @Description: TODO
 * @author: hl
 * @date: 2016年7月2日 上午8:46:43
 */
public class PayCountBolt implements IRichBolt
{
    private static final long serialVersionUID = 7591260982890048043L;
    
    private static final Logger LOG = LoggerFactory.getLogger(PayCountBolt.class);
    
    private OutputCollector collector;
    
    private static Map<Long, Double> pcsum = null;
    
    private static Map<Long, Double> wiresum = null;
    
    private static double pcall = 0.0;
    
    private static double wireall = 0.0;
    
    private static int timeoffset = 60;
    
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
        pcsum = new ConcurrentSkipListMap<Long, Double>();
        wiresum = new ConcurrentSkipListMap<Long, Double>();
        // timeoffset = (Integer)stormConf.get("TIME_OFFSET");
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                // TODO Auto-generated method stub
                while (true)
                {
                    
                    try
                    {
                        Thread.sleep(timeoffset * 1000);
                        // TODO 存储数据库
                        Iterator<?> it = pcsum.keySet().iterator();
                        long time = 0;
                        while (it.hasNext())
                        {
                            time = (Long)it.next();
                            pcall += pcsum.get(time);
                            // save(time, pcsum.get(time), "pc");
                            clearData(time, pcsum);
                            break;
                        }
                        if (wiresum.containsKey(time))
                        {
                            wireall += wiresum.get(time);
                            // save(time, wiresum.get(time), "wire");
                            clearData(time, wiresum);
                        }
                        
                        double ratio = (wireall / pcall);
                        DecimalFormat df = new DecimalFormat("#.##");
                        ratio = Double.parseDouble(df.format(ratio));
                        TairOperatorImpl.write(RaceConfig.prex_ratio + time, ratio);
                        
                    }
                    catch (InterruptedException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                
            }
        }).start();
    }
    
    @Override
    public void execute(Tuple input)
    {
        String type = (String)input.getStringByField("type");
        long time = input.getLongByField("time");
        double money = input.getDoubleByField("money");
        try
        {
            
            if (type.equals("0"))
            {
                
                addPayCount(time, money, pcsum);
                
            }
            else if (type.equals("1"))
            {
                addPayCount(time, money, wiresum);
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
        
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
    
    /**
     * 
     * @Title: dealMessage
     * @Description: TODO
     * @return: void
     */
    
    public void addPayCount(long time, Double money, Map<Long, Double> map)
    {
        
        Double sum = map.get(time);
        
        if (null == sum)
        {
            map.put(time, money);
        }
        else
        {
            map.put(time, sum + money);
        }
    }
    
    /**
     * 
     * @Title: clearData
     * @Description: TODO
     * @param time
     * @param map
     * @return: void
     */
    public synchronized void clearData(long time, Map<Long, Double> map)
    {
        map.remove(time);
        
    }
    
    public void save(long time, double money, String str)
    {
        try
        {
            
            File file = new File("pay.txt");
            
            // if file doesnt exists, then create it
            if (!file.exists())
            {
                file.createNewFile();
            }
            
            // true = append file
            FileWriter fileWritter = new FileWriter(file.getName(), true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.write("pay   " + String.valueOf(time) + "----" + money + "---" + str);
            bufferWritter.write("\n");
            bufferWritter.close();
            
            System.out.println("Done");
            
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    
}
