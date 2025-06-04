package uk.gov.justice.digital.common;

public class TaskDetail {

    private final String token;
    private final boolean ignoreFailure;

    public TaskDetail(String token, boolean ignoreFailure) {
        this.token = token;
        this.ignoreFailure = ignoreFailure;
    }

    public String getToken() {
        return token;
    }

    public boolean ignoreFailure() {
        return ignoreFailure;
    }

}
