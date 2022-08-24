package twitterclass;

import com.twitter.clientlib.model.StreamingTweetResponse;

import java.util.Properties;

public interface TweetsStreamListener {
    void actionOnTweetsStream(StreamingTweetResponse streamingTweet);
}
