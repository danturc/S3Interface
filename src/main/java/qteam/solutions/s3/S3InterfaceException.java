package qteam.solutions.s3;

public class S3InterfaceException extends Exception {

	private static final long serialVersionUID = 8134024744761955931L;

	public S3InterfaceException() {
	}

	public S3InterfaceException(String message) {
		super(message);
	}

	public S3InterfaceException(Throwable cause) {
		super(cause);
	}

	public S3InterfaceException(String message, Throwable cause) {
		super(message, cause);
	}
}
