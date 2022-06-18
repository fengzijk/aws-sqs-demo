package com.fengzijk.awssqsdemo.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import io.awspring.cloud.core.region.RegionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
public class AppConfig {

    @Bean
    AmazonSQSAsync amazonSQS(AWSCredentialsProvider awsCredentialsProvider, RegionProvider regionProvider,
                             ClientConfiguration clientConfiguration) {
        return AmazonSQSAsyncClientBuilder.standard().withCredentials(awsCredentialsProvider)
                .withClientConfiguration(clientConfiguration).withRegion(regionProvider.getRegion().getName()).build();
    }
}
