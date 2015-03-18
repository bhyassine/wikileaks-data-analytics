package exception;

import java.io.IOException;

public class OutputAlreadyExistsException extends IOException {
	public OutputAlreadyExistsException(String error) {
		super(error);
	}
}
