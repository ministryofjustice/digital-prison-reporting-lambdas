package uk.gov.justice.digital;

import java.time.LocalDateTime;

public class TableS3MetaData extends TableS3Location {
    public final Long createdEpochDate;

    public TableS3MetaData(String tableName, String s3Location, Long createdEpochDate) {
        super(tableName, s3Location);
        this.createdEpochDate = createdEpochDate;
    }
}
