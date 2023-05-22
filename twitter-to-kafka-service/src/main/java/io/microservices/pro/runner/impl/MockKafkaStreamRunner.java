package io.microservices.pro.runner.impl;

import io.microservices.pro.config.TwitterToKafkaServiceConfigData;
import io.microservices.pro.exception.TwitterToKafkaServiceException;
import io.microservices.pro.listener.TwitterKafkaStatusListener;
import io.microservices.pro.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;


@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {
    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private static final Random RANDOM = new Random();
    private static final String[] WORDS = getWords();
    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyy";

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\","
            + "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    public MockKafkaStreamRunner(TwitterKafkaStatusListener twitterKafkaStatusListener, TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData) {
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Started Mocking Twitter Data Stream for Keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs){
        Executors.newSingleThreadExecutor().submit(() ->{
           try{
               while(true){
                   String formattedTweetAsJson = getFormattedTweets(keywords, minTweetLength, maxTweetLength);
                   Status status = TwitterObjectFactory.createStatus(formattedTweetAsJson);
                   twitterKafkaStatusListener.onStatus(status);
                   tweetSleep(sleepTimeMs);
               }
           }catch (TwitterException e){ LOG.error("Error Creating twitter Status ", e);}
        });

    }

    private void tweetSleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        }catch (InterruptedException e){
            throw new TwitterToKafkaServiceException("Error While Waiting for new Status to create!!!");
        }
    }

    private String getFormattedTweets(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };
        return formatTweetAsJsonWithParams(params);
    }

    private static String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet  = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private static String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength / 2) tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
        }
        return tweet.toString().trim();
    }

    private static String[] getWords(){
        return new String[]{
                "Lorem",
                "ipsum",
                "dolor",
                "sensectetuer",
                "adipiscina",
                "elit",
                "ascenes",
                "porttitor",
                "conque",
                "Dasse",
                "Besuere",
                "magna",
                "sed",
                "aulviner",
                "ultricies",
                "Durus",
                "libero"
        };
    }
}
