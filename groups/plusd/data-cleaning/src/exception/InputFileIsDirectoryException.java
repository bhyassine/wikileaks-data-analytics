package exception;


import java.io.IOException;

public class InputFileIsDirectoryException extends IOException {
	public InputFileIsDirectoryException(String error) {
		super(error);
	}
}
