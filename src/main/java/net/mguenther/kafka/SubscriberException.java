package net.mguenther.kafka;

import java.io.Serial;

public class SubscriberException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -689043280L;

    SubscriberException(final Throwable cause) {
        super(cause);
    }
}
