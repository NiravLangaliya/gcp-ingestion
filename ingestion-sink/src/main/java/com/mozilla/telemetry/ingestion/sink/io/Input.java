package com.mozilla.telemetry.ingestion.sink.io;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.sink.util.BatchException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;

public abstract class Input {

  abstract void run();

  protected static <T> MessageReceiver getConsumer(Logger logger,
      Function<PubsubMessage, PubsubMessage> decompress,
      Function<PubsubMessage, CompletableFuture<T>> output) {
    // Synchronous CompletableFuture methods are executed by the thread that completes the
    // future, or the current thread if the future is already complete. Use that here to
    // minimize memory usage by doing as much work as immediately possible.
    return (message, consumer) -> CompletableFuture.completedFuture(message).thenApply(decompress)
        .thenCompose(output).whenComplete((result, exception) -> {
          if (exception == null) {
            consumer.ack();
          } else {
            // exception is always a CompletionException caused by another exception
            if (exception.getCause() instanceof BatchException) {
              // only log batch exception once
              ((BatchException) exception.getCause()).handle((batchExc) -> logger.error(
                  String.format("failed to deliver %d messages", batchExc.size),
                  batchExc.getCause()));
            } else {
              // log exception specific to this message
              logger.error("failed to deliver message", exception.getCause());
            }
            consumer.nack();
          }
        });
  }
}
