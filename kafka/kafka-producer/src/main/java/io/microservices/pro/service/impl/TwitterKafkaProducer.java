package io.microservices.pro.service.impl;

import io.microservices.pro.kafka.avro.model.TwitterAvroModel;
import io.microservices.pro.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);
    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOG.info("Send messages='{}' to topic='{}'", message, topicName);
        ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture =  kafkaTemplate.send(topicName, key, message);
        addCallBack(topicName, message, kafkaResultFuture);
    }

    @PreDestroy
    public void close(){
        if(kafkaTemplate != null)
            LOG.info("Closing Kafka Producer");
        kafkaTemplate.destroy();
    }

    private static void addCallBack(String topicName, TwitterAvroModel message, ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
        kafkaResultFuture.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
            @Override
            public void onFailure(Throwable ex) {
                LOG.error("Error while sending message {} to topic {}", message.toString(), topicName, ex);
            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
                RecordMetadata metadata = result.getRecordMetadata();
                LOG.info("Received new metadata. Topic: {}, Partition: {}, Offset: {}, Timestamp: {} and time: {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), System.nanoTime());
            }
        });
    }
}
