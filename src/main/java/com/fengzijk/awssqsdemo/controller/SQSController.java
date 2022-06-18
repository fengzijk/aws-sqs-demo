package com.fengzijk.awssqsdemo.controller;

import io.awspring.cloud.messaging.core.QueueMessagingTemplate;
import io.awspring.cloud.messaging.core.SqsMessageHeaders;
import io.awspring.cloud.messaging.core.TopicMessageChannel;
import io.awspring.cloud.messaging.listener.Acknowledgment;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * <pre>sqs Demo</pre>
 *
 * @author : guozhifeng
 * @date : 2022/6/18 23:17
 */
@RestController
@Slf4j
@RequestMapping(value = "sqs")
public class SQSController {

    @Autowired
    private QueueMessagingTemplate queueMessagingTemplate;


    @GetMapping("/put/{msg}")
    public void putMessagedToQueue(@PathVariable("msg") String message) {
        queueMessagingTemplate.convertAndSend("test",
                MessageBuilder.withPayload(message)
                        .setHeader(TopicMessageChannel.MESSAGE_GROUP_ID_HEADER, "group1")
                        .setHeader(SqsMessageHeaders.SQS_DEDUPLICATION_ID_HEADER, System.currentTimeMillis())
                        .setHeader(SqsMessageHeaders.SQS_DELAY_HEADER, 10)
                        .build());
    }

    @SneakyThrows
    @SqsListener(value = "", deletionPolicy = SqsMessageDeletionPolicy.NEVER)
    public void loadMessagesFromSnsQueue(Acknowledgment acknowledgment, Message<String> message, @Headers MessageHeaders headers) {

        //
        String messageId = Optional.ofNullable(headers.get("MessageId")).orElse("").toString();
        int receiveCount = Integer.parseInt(Optional.ofNullable(headers.get(SqsMessageHeaders.SQS_APPROXIMATE_RECEIVE_COUNT)).orElse(1).toString());
        long sendTimeStamp = Long.parseLong(Optional.ofNullable(headers.get(SqsMessageHeaders.SQS_SENT_TIMESTAMP)).orElse(1).toString());
        String deduplicationId=Optional.ofNullable(headers.get(SqsMessageHeaders.SQS_DEDUPLICATION_ID_HEADER)).orElse("").toString();
        String approximateFirstReceiveTimestamp=Optional.ofNullable(headers.get(SqsMessageHeaders.SQS_APPROXIMATE_FIRST_RECEIVE_TIMESTAMP)).orElse("").toString();


        log.info("messageId:{}", messageId);
        log.info("receiveCount:{}", receiveCount);
        log.info("sendTimeStamp:{}", sendTimeStamp);
        log.info("deduplicationId:{}", deduplicationId);
        log.info("approximateFirstReceiveTimestamp:{}", approximateFirstReceiveTimestamp);

        if (receiveCount > 0 ) {
             Object o = acknowledgment.acknowledge().get();
             log.info("o:{}",o.toString());
        }

    }
}
