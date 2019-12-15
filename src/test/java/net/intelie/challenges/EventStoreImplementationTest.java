package net.intelie.challenges;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
