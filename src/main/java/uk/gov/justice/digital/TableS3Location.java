package uk.gov.justice.digital;

public class TableS3Location {
    public final String tableName;
    public final String s3Location;

    public TableS3Location(String tableName, String s3Location) {
        this.tableName = tableName;
        this.s3Location = s3Location;
    }
}
