package uk.gov.justice.digital.clients.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import static uk.gov.justice.digital.common.Utils.DEFAULT_DPR_REGION;

public class DefaultDynamoDbProvider implements DynamoDbProvider {
    @Override
    public AmazonDynamoDB buildClient() {
        return AmazonDynamoDBClientBuilder.standard().withRegion(DEFAULT_DPR_REGION).build();
    }
}
