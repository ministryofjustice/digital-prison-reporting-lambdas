package uk.gov.justice.digital;

import java.util.Objects;

public class TableS3MetaData extends TableS3Location {
    public final Long createdEpochDate;

    public TableS3MetaData(String tableName, String s3Location, Long createdEpochDate) {
        super(tableName, s3Location);
        this.createdEpochDate = createdEpochDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableS3MetaData that = (TableS3MetaData) o;

        return Objects.equals(createdEpochDate, that.createdEpochDate)
                && super.equals(o);
    }

    @Override
    public int hashCode() {
        return createdEpochDate != null ? createdEpochDate.hashCode() : 0;
    }
}
