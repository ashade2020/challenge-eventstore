package net.intelie.challenges;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class EventStoreImplementation implements EventStore {

    /**
     * internal event storage. All major operations (insert, delete, contains and size) are O(1) on HashMap and O(lg n)
     * on ConcurrentSkipListMap. Moreover, ConcurrentSkipListMap supports range queries in O(lg n), where n is the size
     * of the collection. Both ConcurrentHashMap and ConcurrentSkipListMap are thread-safe.
     *
     * The implementation assumes that two events may have the same timestamp if, and only if, they have different types.
     * If a new event is inserted and there exists another event of the same type and with same timestamp, then the
     * new event is ignored (not inserted).
     *
     * Please take a look at test codes within the test folder for more details on the requirements and implemented behaviours.
     */
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Event>> events = new ConcurrentHashMap<>();

    /**
     * Stores an event
     *
     * @param event
     */
    @Override
    public void insert(Event event) {
        ConcurrentSkipListMap<Long, Event> eventsOfThisType;
        if (events.containsKey(event.type())) eventsOfThisType = events.get(event.type());
        else {
            eventsOfThisType = new ConcurrentSkipListMap<>();
            events.put(event.type(), eventsOfThisType);
        }
        // subsequent events with same timestamp and same type are ignored.
        eventsOfThisType.putIfAbsent(event.timestamp(), event);
    }

    /**
     * Removes all events of specific type.
     *
     * @param type
     */
    @Override
    public void removeAll(String type) {
        if (events.containsKey(type)) {
            ConcurrentSkipListMap<Long, Event> removedEntry = events.remove(type);
            removedEntry.clear();
        }
    }

    /**
     * Retrieves an iterator for events based on their type and timestamp.
     *
     * @param type      The type we are querying for.
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     * @return An iterator where all its events have same type as
     * {@param type} and timestamp between {@param startTime}
     * (inclusive) and {@param endTime} (exclusive).
     */
    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        if (!events.containsKey(type)) return EventIteratorImplementation.empty();
        return new EventIteratorImplementation(events.get(type).subMap(startTime, true, endTime, false));
    }
}
