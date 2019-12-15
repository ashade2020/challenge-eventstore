package net.intelie.challenges;

import org.junit.Test;
import org.junit.Assert;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class EventIteratorImplementationTest {
    @Test
    public void emptyIteratorShouldReturnFalseOnMoveNext() {
        EventIterator e = EventIteratorImplementation.empty();
        Assert.assertFalse(e.moveNext());
    }

    @Test
    public void emptyIteratorShouldThrowIllegalStateExceptionOnCurrent() {
        EventIterator e = EventIteratorImplementation.empty();
        try {
            e.current();
            Assert.fail();
        } catch (IllegalStateException ignored) {
            // test ok.
        }
    }

    @Test
    public void emptyIteratorShouldThrowIllegalStateExceptionOnRemove() {
        EventIterator e = EventIteratorImplementation.empty();
        try {
            e.remove();
            Assert.fail();
        } catch (IllegalStateException ignored) {
            // test ok.
        }
    }


    @Test
    public void iteratorShouldReturnOnlyItsTwoElementsInAscendingOrder() {
        // arrange
        EventIterator e = getEventIteratorWithTwoEvents();

        // act + assert
        assertNextEventIsOk(e, 1L);
        assertNextEventIsOk(e, 2L);
        assertNextEventFails(e);
    }

    private void assertNextEventFails(EventIterator e) {
        Assert.assertFalse(e.moveNext());
        assertCurrentFails(e);
    }

    private void assertCurrentFails(EventIterator e) {
        try {
            Event shouldFail = e.current();
            Assert.fail();
        } catch (IllegalStateException ignored) {
            // test ok.
        }
    }

    private void assertRemoveFails(EventIterator e) {
        try {
            e.remove();
            Assert.fail();
        } catch (IllegalStateException ignored) {
            // test ok.
        }
    }

    private void assertNextEventIsOk(EventIterator e, long timestamp) {
        Assert.assertTrue(e.moveNext());
        Event currentEvent = e.current();
        Assert.assertEquals(timestamp, currentEvent.timestamp());
        Assert.assertEquals("t", currentEvent.type());
    }

    private EventIterator getEventIteratorWithTwoEvents() {
        ConcurrentNavigableMap<Long, Event> map = new ConcurrentSkipListMap<>();
        fillMapWithTwoEvents(map);

        return new EventIteratorImplementation(map);
    }

    private void fillMapWithTwoEvents(ConcurrentNavigableMap<Long, Event> map) {
        map.put(1L, new Event("t", 1));
        map.put(2L, new Event("t", 2));
    }

    @Test
    public void iteratorShouldFailWhenCurrentIsCalledBeforeAnyMoveNextCall() {
        // arrange
        EventIterator e = getEventIteratorWithTwoEvents();

        // act + assert
        assertCurrentFails(e);
    }

    @Test
    public void removeShouldDeleteCurrentElementAndNotMoveToNext() {
        // arrange
        ConcurrentNavigableMap<Long, Event> map = new ConcurrentSkipListMap<>();
        fillMapWithTwoEvents(map);
        EventIterator e = new EventIteratorImplementation(map);

        // act
        Assert.assertTrue(e.moveNext());
        e.remove();

        // assert
        Assert.assertFalse(map.containsKey(1L));
        Assert.assertNull(e.current()); // current returns null for the Event was removed.
        assertNextEventIsOk(e, 2L);
        assertNextEventFails(e);
    }

    @Test
    public void removeShouldThrowIllegalStateExceptionWhenCalledBeforeMoveToNext() {
        // arrange
        EventIterator e = getEventIteratorWithTwoEvents();

        // act + assert
        assertRemoveFails(e);
    }
}
