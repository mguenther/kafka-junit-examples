package net.mguenther.kafka;

public interface Subscriber<T> extends Runnable {

    void onEvent(T event);

    void close();

    @Override
    void run();
}
