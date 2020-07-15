package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.StatusException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubLite {

  public static class Read extends Input {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubLite.Read.class);

    @VisibleForTesting
    public Subscriber subscriber;

    /** Constructor. */
    public <T> Read(String subscription, Function<PubsubMessage, CompletableFuture<T>> output,
        Function<SubscriberSettings.Builder, SubscriberSettings.Builder> config,
        Function<PubsubMessage, PubsubMessage> decompress) throws StatusException {
      subscriber = Subscriber.create(config.apply(
          SubscriberSettings.newBuilder().setSubscriptionPath(SubscriptionPath.of(subscription))
              .setReceiver(Input.getConsumer(LOG, decompress, output)))
          .build());
    }

    /** Run the subscriber until terminated. */
    public void run() {
      try {
        subscriber.startAsync();
        subscriber.awaitTerminated();
      } finally {
        subscriber.stopAsync();
      }
    }
  }
}
