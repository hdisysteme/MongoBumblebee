package de.hdi.mongobumblebee.exception;

/**
 * Error while connection to MongoDB
 *
 * @author lstolowski
 * @since 27/07/2014
 */
public class MongoBumblebeeConnectionException extends MongoBumblebeeException {
	
	public MongoBumblebeeConnectionException(String message, Exception baseException) {
		super(message, baseException);
	}
}
