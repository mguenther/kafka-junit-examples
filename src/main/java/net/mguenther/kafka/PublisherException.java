package net.mguenther.kafka;

import java.io.Serial;

final public class PublisherException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -3284902420L;

    PublisherException(final Throwable cause) {
        super(cause);
    }
}
