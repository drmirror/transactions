package net.drmirror.transaction;

public class RollbackException extends RuntimeException {

    private Exception cause = null;
    
    public RollbackException(Exception cause) {
        super("cause: " + cause.getMessage());
        this.cause = cause;
    }

    public Exception getCause() {
        return cause;
    }

}
