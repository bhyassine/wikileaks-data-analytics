package exception;


import java.io.IOException;

public class InputNotDirectoryException extends IOException {
	public InputNotDirectoryException(String error) {
		super(error);
	}
}
