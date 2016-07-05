package com.alibaba.middleware.race;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;

public class RocketMQConfig implements Serializable {
	private static final long serialVersionUID = 4157424979688590880L;

	/**
	 * Unique mark for every JVM instance
	 */
	private String instanceName;
	/**
	 * Group by message actor
	 */
	private String groupId;
	/**
	 * Message topic
	 */
	private String topic;
	/**
	 * Message topic tag
	 */
	private String topicTag;
	/**
	 * Minimal consumer thread count
	 */
	private int consumeThreadMin = 20;
	/**
	 * Maximal consumer thread count
	 */
	private int consumeThreadMax = 64;
	/**
	 * If piled-up message exceeds this value,adjust consumer thread to max
	 * value dynamically
	 */
	private long adjustThreadPoolNumsThreshold = 100000l;
	/**
	 * Local message queue threshold, trigger flow control if exceeds this value
	 */
	private int pullThresholdForQueue = 1024;
	/**
	 * The message size from server for every pull batch
	 */
	private int pullBatchSize = 32;
	/**
	 * Pull interval from server for every pull
	 */
	private long pullInterval = 0;
	/**
	 * Fetch message size from local queue
	 */
	private int consumeMessageBatchMaxSize = 32;
	/**
	 * Consumption of local sequence, will affect performance
	 */
	private boolean ordered;
	/**
	 * The max allowed failures for one single message, skip the failure message
	 * if excesses. -1 means try again until success
	 */
	private int maxFailTimes = 5;

	public RocketMQConfig() {
	}

	public RocketMQConfig(String consumerGroup, String topic, String topicTag) {
		super();
		this.groupId = consumerGroup;
		this.topic = topic;
		this.topicTag = topicTag;
	}

	/**
	 * @return the instanceName
	 */
	public String getInstanceName() {
		return instanceName;
	}

	/**
	 * @param instanceName
	 *            the instanceName to set
	 */
	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	/**
	 * @return the groupId
	 */
	public String getGroupId() {
		return groupId;
	}

	/**
	 * @param groupId
	 *            the groupId to set
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	/**
	 * @return the topic
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * @param topic
	 *            the topic to set
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * @return the topicTag
	 */
	public String getTopicTag() {
		return topicTag;
	}

	/**
	 * @param topicTag
	 *            the topicTag to set
	 */
	public void setTopicTag(String topicTag) {
		this.topicTag = topicTag;
	}

	/**
	 * @return the consumeThreadMin
	 */
	public int getConsumeThreadMin() {
		return consumeThreadMin;
	}

	/**
	 * @param consumeThreadMin
	 *            the consumeThreadMin to set
	 */
	public void setConsumeThreadMin(int consumeThreadMin) {
		this.consumeThreadMin = consumeThreadMin;
	}

	/**
	 * @return the consumeThreadMax
	 */
	public int getConsumeThreadMax() {
		return consumeThreadMax;
	}

	/**
	 * @param consumeThreadMax
	 *            the consumeThreadMax to set
	 */
	public void setConsumeThreadMax(int consumeThreadMax) {
		this.consumeThreadMax = consumeThreadMax;
	}

	/**
	 * @return the adjustThreadPoolNumsThreshold
	 */
	public long getAdjustThreadPoolNumsThreshold() {
		return adjustThreadPoolNumsThreshold;
	}

	/**
	 * @param adjustThreadPoolNumsThreshold
	 *            the adjustThreadPoolNumsThreshold to set
	 */
	public void setAdjustThreadPoolNumsThreshold(
			long adjustThreadPoolNumsThreshold) {
		this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
	}

	/**
	 * @return the pullThresholdForQueue
	 */
	public int getPullThresholdForQueue() {
		return pullThresholdForQueue;
	}

	/**
	 * @param pullThresholdForQueue
	 *            the pullThresholdForQueue to set
	 */
	public void setPullThresholdForQueue(int pullThresholdForQueue) {
		this.pullThresholdForQueue = pullThresholdForQueue;
	}

	/**
	 * @return the pullBatchSize
	 */
	public int getPullBatchSize() {
		return pullBatchSize;
	}

	/**
	 * @param pullBatchSize
	 *            the pullBatchSize to set
	 */
	public void setPullBatchSize(int pullBatchSize) {
		this.pullBatchSize = pullBatchSize;
	}

	/**
	 * @return the pullInterval
	 */
	public long getPullInterval() {
		return pullInterval;
	}

	/**
	 * @param pullInterval
	 *            the pullInterval to set
	 */
	public void setPullInterval(long pullInterval) {
		this.pullInterval = pullInterval;
	}

	/**
	 * @return the consumeMessageBatchMaxSize
	 */
	public int getConsumeMessageBatchMaxSize() {
		return consumeMessageBatchMaxSize;
	}

	/**
	 * @param consumeMessageBatchMaxSize
	 *            the consumeMessageBatchMaxSize to set
	 */
	public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
		this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
	}

	/**
	 * @return the ordered
	 */
	public boolean isOrdered() {
		return ordered;
	}

	/**
	 * @param ordered
	 *            the ordered to set
	 */
	public void setOrdered(boolean ordered) {
		this.ordered = ordered;
	}

	/**
	 * @return the maxFailTimes
	 */
	public int getMaxFailTimes() {
		return maxFailTimes;
	}

	/**
	 * @param maxFailTimes
	 *            the maxFailTimes to set
	 */
	public void setMaxFailTimes(int maxFailTimes) {
		this.maxFailTimes = maxFailTimes;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}