package com.alibaba.otter.manager.biz.common.alarm;

import com.alibaba.otter.manager.biz.config.channel.dal.ChannelDAO;
import com.alibaba.otter.manager.biz.config.channel.dal.dataobject.ChannelDO;
import com.alibaba.otter.manager.biz.config.pipeline.dal.PipelineDAO;
import com.alibaba.otter.manager.biz.config.pipeline.dal.dataobject.PipelineDO;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

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

    private PipelineDAO pipelineDAO;

    private ChannelDAO channelDAO;

    private Map<Long, String> pipelineInfos = new ConcurrentHashMap<Long, String>(128);

    @Override
    public void doSend(AlarmMessage data) throws Exception {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        HttpPost httpPost = new HttpPost(dingTalkUrl);

        StringBuilder sb = new StringBuilder();
        sb.append(TITLE).append("@").append(clusterName);
        if (data.getPipelineId() > 0) {
            if (pipelineInfos.containsKey(data.getPipelineId())) {
                sb.append(", channel:").append(pipelineInfos.get(data.getPipelineId()));
            } else {
                PipelineDO pipelineDO = pipelineDAO.findById(data.getPipelineId());
                if (pipelineDO != null) {
                    ChannelDO channelDO = channelDAO.findById(pipelineDO.getChannelId());
                    if (channelDO != null) {
                        sb.append(", channel:").append(channelDO.getName());
                        pipelineInfos.put(data.getPipelineId(), channelDO.getName());
                    }
                }
            }
        }

        StringEntity entity = new StringEntity("{'msgtype':'text','text':{'content':'" + sb.toString() + ":" + data.getMessage() + "'}}", "UTF-8");
        httpPost.setEntity(entity);

        httpPost.setHeader("Content-Type", "application/json;charset=utf8");

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            HttpEntity responseEntity = response.getEntity();

        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
            if (response != null) {
                response.close();
            }
        }
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

    public PipelineDAO getPipelineDAO() {
        return pipelineDAO;
    }

    public void setPipelineDAO(PipelineDAO pipelineDAO) {
        this.pipelineDAO = pipelineDAO;
    }

    public void setChannelDAO(ChannelDAO channelDAO) {
        this.channelDAO = channelDAO;
    }
}
