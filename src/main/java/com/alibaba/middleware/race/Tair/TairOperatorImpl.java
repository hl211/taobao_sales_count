package com.alibaba.middleware.race.Tair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/ group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl
{
    
    public static DefaultTairManager tairManager = null;
    
    public static void tairOperatorinit()
    {
        tairManager = new DefaultTairManager();
        tairManager.setGroupName(RaceConfig.TairGroup);
        
        List<String> confServers = new ArrayList<String>();
        confServers.add(RaceConfig.TairConfigServer);
        confServers.add(RaceConfig.TairSalveConfigServer);
        tairManager.setConfigServerList(confServers);
        tairManager.init();
    }
    
    public static boolean write(Serializable key, Serializable value)
    {
        if (tairManager == null)
        {
            tairOperatorinit();
        }
        ResultCode result = tairManager.put(RaceConfig.TairNamespace, key, value);
        if (!result.isSuccess())
        {
            return false;
        }
        return true;
    }
    
    public static Object get(Serializable key)
    {
        if (tairManager == null)
        {
            tairOperatorinit();
        }
        return null;
    }
    
    public static boolean remove(Serializable key)
    {
        if (tairManager == null)
        {
            tairOperatorinit();
        }
        return false;
    }
    
    public static void close()
    {
    }
    
}
