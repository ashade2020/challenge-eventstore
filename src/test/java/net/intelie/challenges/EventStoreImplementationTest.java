package net.intelie.challenges;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class EventStoreImplementationTest {
    @Test
    public void queryShouldReturnSingleElementWhenInsertIsCalledOnce() {
        // arrange
        Event event1 = new Event("t1", 1L);
        EventStore e = createEventStore(Collections.singletonList(event1));

        // act
        EventIterator result = e.query("t1", 0L, 10L);

        // assert
        assertQueryResultIsOk(result, Collections.singletonList(event1));
    }

    @Test
    public void queryShouldReturnSingleElementWhenWhenOnlyOneFitsIntoTimestampRange() {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t1", 10L);
        EventStore e = createEventStore(Arrays.asList(event1, event2));

        // act
        EventIterator result = e.query("t1", 1L, 10L);

        // assert
        assertQueryResultIsOk(result, Collections.singletonList(event1));
    }

    @Test
    public void queryShouldReturnAllElementsWhenAllFitsIntoTimestampRange() {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t1", 10L);
        EventStore e = createEventStore(Arrays.asList(event1, event2));

        // act
        EventIterator result = e.query("t1", 1L, 11L);

        // assert
        assertQueryResultIsOk(result, Arrays.asList(event1, event2));
    }

    @Test
    public void queryShouldReturnNoElementsWhenNoneFitsIntoTimestampRange() {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t1", 10L);
        EventStore e = createEventStore(Arrays.asList(event1, event2));

        // act
        EventIterator result = e.query("t1", 2L, 10L);

        // assert
        assertQueryResultIsOk(result, Collections.emptyList());
    }

    @Test
    public void queryShouldReturnNoElementsWhenWhenLookingForMissingType() {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t10", 10L);
        EventStore e = createEventStore(Arrays.asList(event1, event2));

        // act
        EventIterator result = e.query("t2", 0L, 100L);

        // assert
        assertQueryResultIsOk(result, Collections.emptyList());
    }

    @Test
    public void queryShouldReturnNoElementsWhenWhenRemoveAllIsCalled() {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t10", 10L);
        EventStore e = createEventStore(Arrays.asList(event1, event2));
        e.removeAll("t1");

        // act
        EventIterator result = e.query("t1", 0L, 100L);

        // assert
        assertQueryResultIsOk(result, Collections.emptyList());
    }

    @Test
    public void queryShouldReturnSingleElementWhenWhenRemoveAllIsCalledOnAnotherType() {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t10", 10L);
        EventStore e = createEventStore(Arrays.asList(event1, event2));
        e.removeAll("t1");

        // act
        EventIterator result = e.query("t10", 0L, 100L);

        // assert
        assertQueryResultIsOk(result, Collections.singletonList(event2));
    }

    @Test
    public void queryShouldReturnSingleElementWhenDuplicatedEventsAreInserted() {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t10", 10L);
        EventStore e = createEventStore(Arrays.asList(event1, event2, event1));

        // act
        EventIterator result = e.query("t1", 0L, 100L);

        // assert
        assertQueryResultIsOk(result, Collections.singletonList(event1));
    }

    @Test
    public void queryShouldNotReturnElementsRemovedThroughIterator() throws Exception {
        // arrange
        Event event1 = new Event("t1", 1L);
        Event event2 = new Event("t1", 2L);
        Event event3 = new Event("t1", 3L);
        EventStore e = createEventStore(Arrays.asList(event1, event2, event3));
        EventIterator result = e.query("t1", 0L, 100L);
        result.moveNext();
        result.remove();
        result.close();

        // act
        result = e.query("t1", 0L, 100L);

        // assert
        assertQueryResultIsOk(result, Arrays.asList(event2, event3));
    }

    private void assertQueryResultIsOk(EventIterator iterator, List<Event> expectedSequence) {
        int count = 0;
        while (iterator.moveNext()) {
            Event event = iterator.current();
            assertEventsAreEqual(expectedSequence.get(count), event);
            count += 1;
        }
        Assert.assertEquals(expectedSequence.size(), count);
    }

    private EventStore createEventStore(List<Event> events) {
        EventStore e = new EventStoreImplementation();
        for (Event ev : events) e.insert(ev);
        return e;
    }

    private void assertEventsAreEqual(Event eventExpected, Event eventRealized) {
        Assert.assertEquals(eventExpected.type(), eventRealized.type());
        Assert.assertEquals(eventExpected.timestamp(), eventRealized.timestamp());
    }
}
