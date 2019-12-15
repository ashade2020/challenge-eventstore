package net.intelie.challenges;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;

public class EventIteratorImplementation implements EventIterator {
    private final ConcurrentNavigableMap<Long, Event> subMap;
    private final Iterator<Long> keySetIterator;
    private Long currentKey = null;

    public EventIteratorImplementation(ConcurrentNavigableMap<Long, Event> subMap) {
        this.subMap = subMap;
        if (subMap != null)
            this.keySetIterator = subMap.keySet().iterator();
        else
            keySetIterator = null;
    }

    public static EventIterator empty() {
        return new EventIteratorImplementation(null);
    }

    /**
     * Move the iterator to the next event, if any.
     *
     * @return false if the iterator has reached the end, true otherwise.
     */
    @Override
    public boolean moveNext() {
        if (subMap == null || keySetIterator == null)
            return false;
        if (keySetIterator.hasNext()) {
            currentKey = keySetIterator.next();
            return true;
        } else {
            currentKey = null;
            return false;
        }
    }

    /**
     * Gets the current event ref'd by this iterator.
     *
     * @return the event itself.
     * @throws IllegalStateException if {@link #moveNext} was never called
     *                               or its last result was {@code false}.
     */
    @Override
    public Event current() {
        if (isInvalidState())
            throw new IllegalStateException();
        return subMap.get(currentKey);
    }

    /**
     * Remove current event from its store.
     *
     * @throws IllegalStateException if {@link #moveNext} was never called
     *                               or its last result was {@code false}.
     */
    @Override
    public void remove() {
        if (isInvalidState())
            throw new IllegalStateException();
        subMap.remove(currentKey);
    }

    private boolean isInvalidState() {
        return subMap == null || keySetIterator == null || currentKey == null;
    }

    @Override
    public void close() throws Exception {
        // nothing to do here.
    }
}
