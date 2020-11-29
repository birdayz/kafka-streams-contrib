package de.nerden.kafka.streams.processor.src.test.java.de.nerden.kafka.streams.processor;

import de.nerden.kafka.streams.processor.AsyncProcessorTest;
import de.nerden.kafka.streams.processor.BatchingProcessorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        AsyncProcessorTest.class,
        BatchingProcessorTest.class,
})
public class AllTests {}
