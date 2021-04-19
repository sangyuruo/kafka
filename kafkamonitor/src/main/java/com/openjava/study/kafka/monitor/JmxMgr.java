package com.openjava.study.kafka.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hidden on 2016/12/8.
 */
public class JmxMgr {
    private static Logger log = LoggerFactory.getLogger(JmxMgr.class);
    private static List<JmxConnection> conns = new ArrayList<JmxConnection>();

    public static boolean init(List<String> ipPortList, boolean newKafkaVersion) {
        for (String ipPort : ipPortList) {
            log.info("init jmxConnection [{}]", ipPort);
            JmxConnection conn = new JmxConnection(newKafkaVersion, ipPort);
            boolean bRet = conn.init();
            if (!bRet) {
                log.error("init jmxConnection error");
                return false;
            }
            conns.add(conn);
        }
        return true;
    }

    public static void printController(){
        for (JmxConnection conn : conns) {
            try {
                conn.printController();
            } catch (IntrospectionException e) {
                e.printStackTrace();
            } catch (ReflectionException e) {
                e.printStackTrace();
            } catch (InstanceNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (MalformedObjectNameException e) {
                e.printStackTrace();
            }
        }
    }

    public static long getMsgInCountPerSec(String topicName) {
        long val = 0;
        for (JmxConnection conn : conns) {
            long temp = conn.getMsgInCountPerSec(topicName);
            val += temp;
        }
        return val;
    }

    public static double getMsgInTpsPerSec(String topicName) {
        double val = 0;
        for (JmxConnection conn : conns) {
            double temp = conn.getMsgInTpsPerSec(topicName);
            val += temp;
        }
        return val;
    }

    public static Map<Integer, Long> getEndOffset(String topicName) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for (JmxConnection conn : conns) {
            Map<Integer, Long> tmp = conn.getTopicEndOffset(topicName);
            if (tmp == null) {
                log.warn("get topic endoffset return null, topic {}", topicName);
                continue;
            }
            for (Integer parId : tmp.keySet()) {//change if bigger
                if (!map.containsKey(parId) || (map.containsKey(parId) && (tmp.get(parId) > map.get(parId)))) {
                    map.put(parId, tmp.get(parId));
                }
            }
        }
        return map;
    }

    public static void main(String[] args) {
        List<String> ipPortList = new ArrayList<String>();
        ipPortList.add("10.39.52.72:7010");
//ipPortList.add("xx.101.130.2:9999");
        JmxMgr.init(ipPortList, true);

        String topicName = "output_batch_msg";
        System.out.println(getMsgInCountPerSec(topicName));
        System.out.println(getMsgInTpsPerSec(topicName));
        System.out.println(getEndOffset(topicName));

        printController();
    }
}
