package de.nerden.kafka.streams.processor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        AsyncProcessorTest.class,
        BatchingProcessorTest.class,
})
public class AllTest {

}
