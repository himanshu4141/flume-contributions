/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.flume.source;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * A Flume Source, which pulls data from Twitter's streaming API. Currently,
 * this only supports pulling from the sample API, and only gets new status
 * updates.
 */
public class TwitterSource extends AbstractSource
    implements EventDrivenSource, Configurable {

  private static final Logger logger =
      LoggerFactory.getLogger(TwitterSource.class);

  private String[] keywords;

  /** The actual Twitter stream. It's set up to collect raw JSON data */
  private  TwitterStream twitterStream;

  /**
   * The initialization method for the Source. The context contains all the
   * Flume configuration info, and can be used to retrieve any configuration
   * values necessary to set up the Source.
   */
  @Override
  public void configure(Context context) {
    /** Information necessary for accessing the Twitter API */
    String consumerKey = context.getString(TwitterSourceConstants.CONSUMER_KEY_KEY);
    String consumerSecret = context.getString(TwitterSourceConstants.CONSUMER_SECRET_KEY);
    String accessToken = context.getString(TwitterSourceConstants.ACCESS_TOKEN_KEY);
    String accessTokenSecret = context.getString(TwitterSourceConstants.ACCESS_TOKEN_SECRET_KEY);

    String keywordString = context.getString(TwitterSourceConstants.KEYWORDS_KEY, "");
    keywords = keywordString.split(",");
    for (int i = 0; i < keywords.length; i++) {
      keywords[i] = keywords[i].trim();
    }

    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setOAuthConsumerKey(consumerKey);
    cb.setOAuthConsumerSecret(consumerSecret);
    cb.setOAuthAccessToken(accessToken);
    cb.setOAuthAccessTokenSecret(accessTokenSecret);
    cb.setJSONStoreEnabled(true);
    cb.setIncludeEntitiesEnabled(true);

    logger.debug("Setting up Twitter sample stream using consumer key {} and" +
          " access token {}", new String[] { consumerKey, accessToken });
	  
    twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
  }

  /**
   * Start processing events. This uses the Twitter Streaming API to sample
   * Twitter, and process tweets.
   */
  @Override
  public void start() {
    // The channel is the piece of Flume that sits between the Source and Sink,
    // and is used to process events.
    final ChannelProcessor channel = getChannelProcessor();

    final Map<String, String> headers = new HashMap<String, String>();

    // The StatusListener is a twitter4j API, which can be added to a Twitter
    // stream, and will execute methods every time a message comes in through
    // the stream.
    StatusListener listener = new StatusListener() {
      // The onStatus method is executed every time a new tweet comes in.
      public void onStatus(Status status) {
        // The EventBuilder is used to build an event using the headers and
        // the raw JSON of a tweet
        logger.debug(status.getUser().getScreenName() + ": " + status.getText());

        headers.put("timestamp", String.valueOf(status.getCreatedAt().getTime()));
        Event event = EventBuilder.withBody(
            DataObjectFactory.getRawJSON(status).getBytes(), headers);
        channel.processEvent(event);
      }

      // This listener will ignore everything except for new tweets
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) { /** no-op */ }
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) { /** no-op */ }
      public void onScrubGeo(long userId, long upToStatusId) { /** no-op */ }
      
      public void onException(Exception ex) {
    	  logger.error("Stream Error ", ex);
      }
      
      public void onStallWarning(StallWarning warning) {
    	  int percentFull = warning.getPercentFull();
    	  logger.warn("Stall Warning Received " , warning);
    	  if(percentFull > 95){
    		  logger.warn("Stallwarning Stream full more han 95 %. Going to wait for 2 minutes");
    		  try {
				Thread.sleep(2 * 60 * 000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	  }
      }
    };


    // Set up the stream's listener (defined above),
    twitterStream.addListener(listener);

    // Set up a filter to pull out industry-relevant tweets
    if (keywords.length == 0) {
      logger.debug("Starting up Twitter sampling...");
      twitterStream.sample();
    } else {
      logger.debug("Starting up Twitter filtering...");

      FilterQuery query = new FilterQuery().track(keywords);
      twitterStream.filter(query);
    }
    super.start();
  }

  /**
   * Stops the Source's event processing and shuts down the Twitter stream.
   */
  @Override
  public void stop() {
    logger.debug("Shutting down Twitter sample stream...");
    twitterStream.shutdown();
    super.stop();
  }
}
