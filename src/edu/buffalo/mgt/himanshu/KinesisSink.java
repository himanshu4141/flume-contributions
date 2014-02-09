/**
 * 
 */
package edu.buffalo.mgt.himanshu;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

/**
 * A Sink for Amazon <a href="http://aws.amazon.com/kinesis/">Kinesis</a>
 * Mandatory configuration parameters are - AWS acceess key, secret key, Kinesis
 * Stream name , No. of shards , Data partition key as inputs
 * 
 * @author Himanshu
 */
public class KinesisSink extends AbstractSink implements Configurable {

	private static final int DEFAULT_STREAM_SIZE = 2;
	private static final Logger logger = LoggerFactory.getLogger(KinesisSink.class);
	
	private String awsAccessKey;
	private String awsSecretKey;

	private String partitionKey;
	private String streamName;
	private Integer streamsize;

	private static AmazonKinesisClient kinesisClient;
	private SinkCounter sinkCounter;

	@Override
	public Status process() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		Status result = Status.READY;
		try {
			transaction.begin();
			int eventAttemptCounter = 0;
			for (int i = 0; i < streamsize; i++) {
				event = channel.take();
				if (event != null) {
					sinkCounter.incrementEventDrainAttemptCount();
					PutRecordRequest putRecordRequest = new PutRecordRequest();
					putRecordRequest.setStreamName(streamName);
					putRecordRequest.setData(ByteBuffer.wrap(event.getBody()));
					putRecordRequest.setPartitionKey(partitionKey);
					PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
					logger.info("Added Record {} to Stream ", putRecordResult);
				} else {
					result = Status.BACKOFF;
					break;
				}
			}
			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
		} catch (Exception ex) {
			transaction.rollback();
			throw new EventDeliveryException("Failed to process transaction", ex);
		} finally {
			transaction.close();
		}
		return result;
	}

	@Override
	public void configure(Context context) {
		awsAccessKey = context.getString("aws.accesskey");
		awsSecretKey = context.getString("aws.secretkey");

		partitionKey = context.getString("aws.partitionkey");
		streamName = context.getString("aws.streamname");
		streamsize = context.getInteger("aws.streamsize",DEFAULT_STREAM_SIZE);
		
		checkNotNull(awsAccessKey, "AWS Access key may not be null");
		checkNotNull(awsSecretKey, "AWS Secret key may not be null");

		checkNotNull(partitionKey, "Kinesis record Partition key may not be null");
		checkNotNull(streamName, "Kinesis Stream Name may not be null");

		
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

		AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
		kinesisClient = new AmazonKinesisClient(credentials);

		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(10);
		ListStreamsResult listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();
		while (listStreamsResult.isHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
			}

			listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
			streamNames.addAll(listStreamsResult.getStreamNames());

		}

		if (!streamNames.contains(streamName)) {
			logger.info("Stream {} doesn't exist, creating the stream", streamName);

			CreateStreamRequest createStreamRequest = new CreateStreamRequest();
			createStreamRequest.setStreamName(streamName);
			createStreamRequest.setShardCount(streamsize);

			kinesisClient.createStream(createStreamRequest);

			// The stream is now being created.
			waitForStreamToBecomeAvailable(streamName);
		}else {
			sinkCounter.incrementConnectionCreatedCount();
		}
	}

	private void checkNotNull(Object val, String errmsg) {
		if (val == null) {
			throw new IllegalArgumentException(errmsg);
		}
	}
	
	private void waitForStreamToBecomeAvailable(String myStreamName) {

		logger.info("Waiting for {} to become ACTIVE",myStreamName);

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 20);
			} catch (InterruptedException e) {
			}
			try {
				DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
				describeStreamRequest.setStreamName(myStreamName);

				describeStreamRequest.setLimit(10);
				DescribeStreamResult describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

				String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
				logger.info(" {} - current state: " , streamStatus);
				if (streamStatus.equals("ACTIVE")) {
					sinkCounter.incrementConnectionCreatedCount();
					return;
				}
			} catch (AmazonServiceException ase) {
				sinkCounter.incrementConnectionFailedCount();
				if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) {
					throw ase;
				}
				throw new RuntimeException("Stream " + myStreamName + " never went active");
			}
		}
	}

	@Override
	public void start() {
		sinkCounter.start();
		super.start();
		logger.info("KinesisSink {} started.",getName());
	}

	@Override
	public void stop() {
		sinkCounter.stop();
		super.stop();
		sinkCounter.incrementConnectionClosedCount();
		logger.info("KinesisSink {} stopped. Event metrics: {}", getName(), sinkCounter);
	}
}
