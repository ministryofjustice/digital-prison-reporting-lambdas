package uk.gov.justice.digital;

public class TableS3Location {
    public final String tableName;
    public final String s3Location;

    public TableS3Location(String tableName, String s3Location) {
        this.tableName = tableName;
        this.s3Location = s3Location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableS3Location that = (TableS3Location) o;

        if (!tableName.equals(that.tableName)) return false;
        return s3Location.equals(that.s3Location);
    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + s3Location.hashCode();
        return result;
    }
}
