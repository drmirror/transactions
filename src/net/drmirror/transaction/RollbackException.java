package net.drmirror.transaction;

public class RollbackException extends RuntimeException {

    private Exception cause = null;
    
    public RollbackException(Exception cause) {
        this.cause = cause;
    }

    public Exception getCause() {
        return cause;
    }

}
