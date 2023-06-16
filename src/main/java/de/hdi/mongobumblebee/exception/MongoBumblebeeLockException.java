package de.hdi.mongobumblebee.exception;

/**
 * Error while can not obtain process lock
 */
public class MongoBumblebeeLockException extends MongoBumblebeeException {
	
	public MongoBumblebeeLockException(String message) {
		super(message);
	}
}
