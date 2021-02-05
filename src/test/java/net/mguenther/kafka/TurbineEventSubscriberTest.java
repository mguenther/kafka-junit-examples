package net.mguenther.kafka;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.SendKeyValuesTransactional.inTransaction;
import static org.assertj.core.api.Assertions.assertThat;

public class TurbineEventSubscriberTest {

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
    void shouldConsumeTurbineRegisteredEvent() throws Exception {

        final var latch = new CountDownLatch(1);
        final var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final var consumer = new InstrumentingTurbineEventSubscriber("turbine-events", config, latch);
        final var consumerThread = new Thread(consumer);

        consumerThread.start();

        final var event = new TurbineRegisteredEvent("1a5c6012", 49.875114, 8.978702);
        final var record = Collections.singletonList(new KeyValue<>(event.getTurbineId(), event));

        kafka.send(inTransaction("turbine-events", record)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TurbineEventSerializer.class.getName()));

        latch.await(10, TimeUnit.SECONDS);

        assertThat(consumer.getReceivedEvents().size()).isEqualTo(1);

        consumer.close();
        consumerThread.join(TimeUnit.SECONDS.toMillis(1));
    }

    @Test
    void shouldNotSeeEventsOfFailedTransaction() throws Exception {

        final var latch = new CountDownLatch(1);
        final var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final var consumer = new InstrumentingTurbineEventSubscriber("turbine-events", config, latch);
        final var consumerThread = new Thread(consumer);

        consumerThread.start();

        final var event = new TurbineRegisteredEvent("1a5c6012", 49.875114, 8.978702);
        final var record = Collections.singletonList(new KeyValue<>(event.getTurbineId(), event));

        kafka.send(inTransaction("turbine-events", record)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TurbineEventSerializer.class.getName())
                .failTransaction());

        latch.await(10, TimeUnit.SECONDS);

        assertThat(consumer.getReceivedEvents().size()).isEqualTo(0);

        consumer.close();
        consumerThread.join(TimeUnit.SECONDS.toMillis(1));
    }

    static class InstrumentingTurbineEventSubscriber extends TurbineEventSubscriber {

        private final List<TurbineEvent> receivedEvents = new ArrayList<>();

        private final CountDownLatch latch;

        public InstrumentingTurbineEventSubscriber(final String topic,
                                                   final Map<String, Object> userSuppliedConfig,
                                                   final CountDownLatch latch) {
            super(topic, userSuppliedConfig);
            this.latch = latch;
        }

        @Override
        public void onEvent(final TurbineEvent event) {
            super.onEvent(event);
            receivedEvents.add(event);
            latch.countDown();
        }

        public List<TurbineEvent> getReceivedEvents() {
            return Collections.unmodifiableList(receivedEvents);
        }
    }
}
