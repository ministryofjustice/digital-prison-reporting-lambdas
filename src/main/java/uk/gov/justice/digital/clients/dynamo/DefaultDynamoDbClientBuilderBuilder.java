package uk.gov.justice.digital.clients.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;


public class DefaultDynamoDbClientBuilderBuilder implements DynamoDbClientBuilder {

    public DefaultDynamoDbClientBuilderBuilder(){}

    @Override
    public AmazonDynamoDB buildClient(String region) {
        return AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
    }
}
