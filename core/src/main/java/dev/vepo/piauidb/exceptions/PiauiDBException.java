package dev.vepo.piauidb.exceptions;

public class PiauiDBException extends RuntimeException {

    public PiauiDBException(String message, Exception cause) {
        super(message, cause);
    }

    public PiauiDBException(String message) {
        super(message);
    }
    
}
