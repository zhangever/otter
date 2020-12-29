package com.alibaba.otter.manager.biz.common.alarm;

import com.alibaba.otter.manager.biz.config.channel.ChannelService;
import com.alibaba.otter.manager.biz.config.pipeline.PipelineService;
import com.alibaba.otter.shared.common.model.config.channel.Channel;
import com.alibaba.otter.shared.common.model.config.channel.ChannelStatus;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 发送钉钉机器人进行报警
 *
 * @author Ever 2020-11-26 上午12:30:04
 * @since 4.2.18
 */
public class DingTalkAlarmService extends AbstractAlarmService {

    private static final String TITLE = "otter同步告警:";
    /**
     * 钉钉机器人地址
     */
    private String dingTalkUrl;
    /**
     * 告警集群名字
     */
    private String clusterName;

    /**
     * 监控同步任务名字
     */


    private PipelineService pipelineService;

    private ChannelService channelService;

    /**
     * key = pipelineId， value=channelName
     */
    private Map<Long, String> channelInfos = new ConcurrentHashMap<Long, String>(128);

    /**
     * 待监控同步状态的channel名字
     */
    private String channelName4alarms;
    /**
     * key=channelId, value=Channel
     */
    private Map<Long, Channel> channel4alarmInfos = new HashMap<Long, Channel>(32);
    private Set<Long> errorChannels = new HashSet<Long>(8);

    @Override
    public void doSend(AlarmMessage data) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(TITLE).append("@").append(clusterName);
        if (data.getPipelineId() > 0) {
            if (channelInfos.containsKey(data.getPipelineId())) {
                sb.append(", channel:").append(channelInfos.get(data.getPipelineId()));
            } else {
                Pipeline pipeline = pipelineService.findById(data.getPipelineId());
                if (pipeline != null) {
                    Channel channel = channelService.findById(pipeline.getChannelId());
                    if (channel != null) {
                        sb.append(", channel:").append(channel.getName());
                        channelInfos.put(data.getPipelineId(), channel.getName());
                    }
                }
            }
        }

        _doSend("{'msgtype':'text','text':{'content':'" + sb.toString() + ":" + data.getMessage() + "'}}", dingTalkUrl);
    }

    private void _doSend(String msg, String url) throws Exception {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        HttpPost httpPost = new HttpPost(dingTalkUrl);

        StringEntity entity = new StringEntity(msg, "UTF-8");
        httpPost.setEntity(entity);

        httpPost.setHeader("Content-Type", "application/json;charset=utf8");

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
            if (response != null) {
                response.close();
            }
        }
    }

    public Runnable getExtMonitorJob() {
        if (channelName4alarms != null && !channelName4alarms.trim().equals("")) {
            for (String channelName : channelName4alarms.split(",")) {
                for (Channel channel : channelService.listAll()) {
                    if (channel.getName().equals(channelName)) {
                        channel4alarmInfos.put(channel.getId(), channel);
                        break;
                    }
                }
            }
        }

        return new Runnable() {
            @Override
            public void run() {
                Map<Long, ChannelStatus> status = channelService.getChannelStatus(channel4alarmInfos.keySet());
                StringBuilder recoverSb = new StringBuilder();
                StringBuilder failedSb = new StringBuilder();
                for (Map.Entry<Long, ChannelStatus> channelStatusEntry: status.entrySet()) {
                    switch (channelStatusEntry.getValue()) {
                        case START:
                            if (errorChannels.contains(channelStatusEntry.getKey())) {
                                errorChannels.remove(channelStatusEntry.getKey());
                                recoverSb.append(channel4alarmInfos.get(channelStatusEntry.getKey()).getName()).append(",");
                            }
                            break;
                        case STOP:
                        case PAUSE:
                            if (!errorChannels.contains(channelStatusEntry.getKey())) {
                                errorChannels.add(channelStatusEntry.getKey());
                                failedSb.append(channel4alarmInfos.get(channelStatusEntry.getKey()).getName()).append(",");
                            }
                            break;
                    }
                }

                String failedChannels = "";
                if (failedSb.length() > 0) {
                    failedChannels = "以下channel同步已中断:" + failedSb.toString();
                }
                String recoverChnnels = "";
                if (recoverSb.length() > 0) {
                    recoverChnnels = "\n以下channel已恢复同步:" + recoverSb.toString();
                }

                String finalMsg = failedChannels + recoverChnnels;
                if (finalMsg.length() > 0) {
                    try {
                        _doSend("{'msgtype':'text','text':{'content':'" + TITLE + "@" + clusterName + finalMsg + "'}}", dingTalkUrl);
                        logger.info("同步状态告警信息已发送:" + finalMsg);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        };
    }

    public String getDingTalkUrl() {
        return dingTalkUrl;
    }

    public void setDingTalkUrl(String dingTalkUrl) {
        this.dingTalkUrl = dingTalkUrl;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setChannelName4alarms(String channelName4alarms) {
        this.channelName4alarms = channelName4alarms;
    }
    public void setChannelService(ChannelService channelService) {
        this.channelService = channelService;
    }

    public void setPipelineService(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }
}
