package de.hdi.mongobumblebee.exception;

/**
 * @author abelski
 */
public class MongoBumblebeeException extends Exception {
	
	public MongoBumblebeeException(String message) {
		super(message);
	}

	public MongoBumblebeeException(String message, Throwable cause) {
		super(message, cause);
	}
}
