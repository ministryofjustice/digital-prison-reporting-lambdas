package uk.gov.justice.digital.clients.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public interface DynamoDbClientBuilder {
    AmazonDynamoDB buildClient(String region);
}
