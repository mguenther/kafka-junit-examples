package net.mguenther.kafka;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.assertj.core.api.Assertions.assertThat;

public class TurbineEventPublisherTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void setupKafka() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
    }

    @AfterEach
    void tearDownKafka() {
        kafka.stop();
    }

    @Test
    void shouldPublishTurbineRegisteredEvent() throws Exception {

        final var config = Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        final var publisher = new TurbineEventPublisher("turbine-events", config);
        final var event = new TurbineRegisteredEvent("1a5c6012", 49.875114, 8.978702);

        publisher.log(event);

        final var consumedRecord = kafka.observe(on("turbine-events", 1, TurbineEvent.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TurbineEventDeserializer.class.getName()))
                .stream()
                .findFirst()
                .orElseThrow(AssertionError::new);

        assertThat(consumedRecord.getKey()).isEqualTo("1a5c6012");
        assertThat(consumedRecord.getValue()).isInstanceOf(TurbineRegisteredEvent.class);
    }
}
