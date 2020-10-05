package org.acme.kafka;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@ApplicationScoped
@Named("rebalanced-example.rebalancer")
public class KafkaRebalancedConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private static final Logger LOGGER = Logger.getLogger(KafkaRebalancedConsumerRebalanceListener.class.getName());

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrap;

    @ConfigProperty(name = "mp.messaging.incoming.txn.group.id")
    String groupId;

    /**
     * When receiving a list of partitions will search for the earliest offset within 10 minutes
     * and seek the consumer to it.
     *
     * @param consumer        underlying consumer
     * @param topicPartitions set of assigned topic partitions
     * @return A {@link Uni} indicating operations complete or failure
     */
    @Override
    public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        long now = System.currentTimeMillis();
        Map<String, Object> props = new HashMap();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "us-kafka-bootstrap-kafka-us.apps.myocp.com:443");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("mp.messaging.incoming.txn.group.id"));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> newOffsets = null;


        long shouldStartAt = now - 60000_000L; //10 minute ago
        try {
           newOffsets = RemoteClusterUtils.translateOffsets(
                    props, bootstrap, groupId, Duration.ofMinutes(1));
           System.out.println("New offsets"+newOffsets);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


        Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> finalNewOffsets = newOffsets;
        return Uni
                .combine()
                .all()
                .unis(topicPartitions
                        .stream()
                        .map(topicPartition -> {
                            for(org.apache.kafka.common.TopicPartition topicPartition1: finalNewOffsets.keySet()) {
                                if (topicPartition1.partition() == topicPartition.getPartition())

                                    LOGGER.info("Assigned " + topicPartition);
                                    return consumer.offsetsForTimes(topicPartition, shouldStartAt)
                                        .onItem()
                                        .invoke(o -> LOGGER.info("Seeking to " + finalNewOffsets.get(topicPartition1).offset()))
                                        .onItem()
                                        .produceUni(o -> consumer
                                                .seek(topicPartition, o == null ? 0L : finalNewOffsets.get(topicPartition1).offset())
                                                .onItem()
                                                .invoke(v -> LOGGER.info("Seeked to " + o))
                                        );
                            }
                            return null;
                        })
                        .collect(Collectors.toList()))
                .combinedWith(a -> null);
    }

    @Override
    public Uni<Void> onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        return Uni
                .createFrom()
                .nullItem();
    }
}