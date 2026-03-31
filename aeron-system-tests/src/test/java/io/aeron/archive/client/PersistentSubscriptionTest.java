/*
 * Copyright 2014-2026 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.PersistentSubscriptionException.Reason;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.RandomWatcher;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.FrameDataLossGenerator;
import io.aeron.test.driver.StreamIdFrameDataLossGenerator;
import io.aeron.test.driver.StreamIdLossGenerator;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static io.aeron.AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID;
import static io.aeron.AeronCounters.FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.SESSION_ID_PARAM_NAME;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.archive.client.PersistentSubscription.FROM_LIVE;
import static io.aeron.archive.client.PersistentSubscription.FROM_START;
import static io.aeron.driver.status.StreamCounter.CHANNEL_OFFSET;
import static io.aeron.driver.status.StreamCounter.STREAM_ID_OFFSET;
import static io.aeron.test.TestContexts.LOCALHOST_CONTROL_REQUEST_CHANNEL;
import static io.aeron.test.TestContexts.LOCALHOST_CONTROL_RESPONSE_CHANNEL;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class PersistentSubscriptionTest
{
    private static final int ONE_KB_MESSAGE_SIZE = 1024 - DataHeaderFlyweight.HEADER_LENGTH;
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int STREAM_ID = 1000;
    private static final String MDC_SUBSCRIPTION_CHANNEL = "aeron:udp?control=localhost:2000";
    private static final String MDC_PUBLICATION_CHANNEL =
        "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=max";
    private static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:2000";
    private static final String MULTICAST_CHANNEL = "aeron:udp?endpoint=224.0.1.1:40456|interface=localhost";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @RegisterExtension
    final RandomWatcher randomWatcher = new RandomWatcher();

    private final MediaDriver.Context driverCtxTpl = new MediaDriver.Context()
        .termBufferSparseFile(true)
        .threadingMode(ThreadingMode.SHARED)
        .publicationTermBufferLength(TERM_LENGTH)
        .ipcTermBufferLength(TERM_LENGTH)
        .dirDeleteOnShutdown(true)
        .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(2))
        .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
        .untetheredWindowLimitTimeoutNs(TimeUnit.SECONDS.toNanos(1))
        .untetheredLingerTimeoutNs(TimeUnit.SECONDS.toNanos(1))
        .publicationLingerTimeoutNs(TimeUnit.SECONDS.toNanos(1))
        .spiesSimulateConnection(true);

    private final Aeron.Context aeronCtxTpl = new Aeron.Context()
        .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);

    private PersistentSubscription.Context persistentSubscriptionCtx;

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private TestMediaDriver driver;
    private File archiveDir;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private PersistentSubscriptionListenerImpl listener;
    private BufferingFragmentHandler fragmentHandler;
    private Archive.Context archiveCtx;
    private AeronArchive.Context aeronArchiveContext;
    private String aeronDirectoryName;

    @BeforeEach
    void setUp()
    {
        aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = driverCtxTpl.clone()
            .aeronDirectoryName(aeronDirectoryName);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(128 * 1024)
            .segmentFileLength(TERM_LENGTH)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .threadingMode(ArchiveThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx.clone());
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDirectoryName));

        aeronArchiveContext = TestContexts.localhostAeronArchive().aeron(aeron);
        aeronArchive = AeronArchive.connect(aeronArchiveContext.clone());

        listener = new PersistentSubscriptionListenerImpl();

        persistentSubscriptionCtx = new PersistentSubscription.Context()
            .aeron(aeron)
            .recordingId(13)
            .startPosition(0)
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(STREAM_ID)
            .replayChannel("aeron:udp?endpoint=localhost:0")
            .replayStreamId(-5)
            .listener(listener)
            .aeronArchiveContext(aeronArchiveContext);

        fragmentHandler = new BufferingFragmentHandler();
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(
            this::closeCloseables,
            aeronArchive,
            aeron,
            archive,
            driver,
            () -> IoUtil.delete(archiveDir, true));
    }

    @Test
    @InterruptAfter(10)
    void shouldNotRequireEventListener()
    {
        final PersistentSubscriptionListenerImpl listener = null; // <-- null listener
        persistentSubscriptionCtx.listener(listener);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));
        }
    }

    @Test
    @InterruptAfter(10)
    void recordingMustExist()
    {
        final int recordingId = 13; // <-- does not exist
        persistentSubscriptionCtx.recordingId(recordingId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.RECORDING_NOT_FOUND,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void recordingStreamMustMatchLiveStream()
    {
        final int liveStreamId = 1001; // <-- not the same as the recorded stream.

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveStreamId(liveStreamId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.STREAM_ID_MISMATCH,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void replayStartPositionMustNotBeBeforeRecordingStartPosition()
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .initialPosition(1024, 0, TERM_LENGTH) // <-- Recording starts at 1024
            .build();

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        final int startPosition = 0; // <-- Trying to start from zero

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(startPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void replayStartPositionMustNotBeAfterRecordingStopPosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);

        final long startPosition = stopPosition * 2; // <-- after end of recording
        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(startPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void replayStartPositionMustNotBeTheSameAsTheRecordingStopPosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        final long stopPosition = persistentPublication.stop();
        assertTrue(stopPosition > 0);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(stopPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(
                Reason.INVALID_START_POSITION,
                ((PersistentSubscriptionException)listener.lastException).reason()
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void replayStartPositionMustNotBeAfterRecordingLivePosition()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        final long recordedPosition = persistentPublication.position();
        assertTrue(recordedPosition > 0);

        final long startPosition = recordedPosition * 2; // <-- ahead of latest recorded position
        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(startPosition);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(null, 1));

            assertEquals(1, listener.errorCount);
            assertEquals(ArchiveException.class, listener.lastException.getClass());
        }
    }

    @InterruptAfter(5)
    @Test
    void replayStartPositionMustBeAlignWithFrameBoundary()
    {
        final PersistentPublication persistentPublication = PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        persistentPublication.persist(generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId)
            .startPosition(ONE_KB_MESSAGE_SIZE - 32)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        systemTestWatcher.ignoreErrorsMatching((log) -> log.contains("does not point to a valid frame"));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::hasFailed, () -> persistentSubscription.controlledPoll(fragmentHandler, 1));
            assertEquals(1, listener.errorCount);
            assertEquals(ArchiveException.class, listener.lastException.getClass());
        }
    }

    @Test
    @InterruptAfter(10)
    void canJoinALiveStreamAtTheBeginning()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertEquals(1, listener.liveJoinedCount);

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            final List<byte[]> messages = generateRandomPayloads(3);
            persistentPublication.persist(messages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldNotReplayOldMessagesWhenStartingFromLive()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> oldMessages = generateRandomPayloads(5);
        persistentPublication.persist(oldMessages);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertEquals(1, listener.liveJoinedCount);

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            final List<byte[]> newMessages = generateRandomPayloads(3);
            persistentPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(newMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, newMessages);
        }
    }

    @InterruptAfter(10)
    @Test
    void shouldJoinLiveWhenItBecomesAvailable()
    {
        // Ensure a recording exists for the stream.
        PersistentPublication persistentPublication = PersistentPublication.create(
            aeronArchive, IPC_CHANNEL, STREAM_ID
        );
        persistentPublication.persist(generateRandomPayloads(1));
        final long recordingId = persistentPublication.recordingId;

        // Stop the live publication.
        persistentPublication.close();

        aeronArchiveContext.messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        persistentSubscriptionCtx
            .startPosition(FROM_LIVE)
            .recordingId(recordingId);

        try (final PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Start trying to join live while the live publication is stopped.
            executeUntil(
                () -> listener.errorCount > 0,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );
            assertFalse(persistentSubscription.isLive());

            // Restart the publication and ensure we join live and can consume messages.
            final PersistentPublication resumedPublication = PersistentPublication.resume(
                aeronArchive, IPC_CHANNEL, STREAM_ID, recordingId
            );

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            final List<byte[]> messages = generateRandomPayloads(5);
            resumedPublication.persist(messages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, messages);
        }
    }

    @InterruptAfter(10)
    @ParameterizedTest
    @ValueSource(longs = { 0, 1024 })
    void canReplayFromStartOfRecording(final long recordingStartPosition)
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .initialPosition(recordingStartPosition, 0, TERM_LENGTH)
            .build();

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        final List<byte[]> oldMessages = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(oldMessages);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_START);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            final List<byte[]> newMessages = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size() + newMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, oldMessages, newMessages);

            assertEquals(1024 + 1024 + recordingStartPosition, persistentPublication.position());
        }
    }

    @InterruptAfter(10)
    @Test
    void canReplayFromSpecifiedPosition()
    {
        final String channel = new ChannelUriStringBuilder()
            .media(IPC_MEDIA)
            .build();

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateFixedPayloads(4, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(firstMessageBatch);
        final List<byte[]> secondMessageBatch = generateFixedPayloads(2, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(secondMessageBatch);


        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(4096);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(secondMessageBatch.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, secondMessageBatch);
        }
    }

    @ParameterizedTest
    @MethodSource("liveChannels")
    @InterruptAfter(5)
    void shouldConsumeLiveOverConfiguredChannel(
        final int fragmentLimit,
        final String persistentSubscriptionChannel,
        final String publicationChannel)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, publicationChannel, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(persistentSubscriptionChannel);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit)
            );
            assertEquals(1, listener.liveJoinedCount);

            final List<byte[]> payloads = generateRandomPayloads(5);
            persistentPublication.persist(payloads);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), () ->
                persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            assertPayloads(fragmentHandler.receivedPayloads, payloads);
        }
    }

    @ParameterizedTest
    @MethodSource("replayChannelsAndStreams")
    @InterruptAfter(5)
    void shouldReplayOverConfiguredChannel(
        final String replayChannel,
        final int replayStreamId,
        final String archiveControlRequestChannel,
        final String archiveControlResponseChannel)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateRandomPayloads(5);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx.aeronArchiveContext()
            .controlRequestChannel(archiveControlRequestChannel)
            .controlResponseChannel(archiveControlResponseChannel);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .replayChannel(replayChannel)
            .replayStreamId(replayStreamId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertTrue(persistentSubscription.isReplaying());

            final MutableLong replaySubPos = new MutableLong(-1);
            final CountersReader counters = aeron.countersReader();
            counters.forEach((counterId1, typeId, keyBuffer, label) ->
            {
                if (typeId == SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID)
                {
                    final int streamId = keyBuffer.getInt(STREAM_ID_OFFSET);
                    if (streamId == replayStreamId)
                    {
                        assertEquals(replayChannel, removeSessionId(keyBuffer.getStringAscii(CHANNEL_OFFSET)));
                        replaySubPos.set(counters.getCounterValue(counterId1));
                    }
                }
            });
            assertEquals(fragmentHandler.position, replaySubPos.get());

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, payloads);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10 })
    @InterruptAfter(5)
    void canJoinLiveWhenLivePositionMatchesReplayPosition(final int fragmentLimit)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> replayMessages = generateRandomPayloads(5);
        persistentPublication.persist(replayMessages);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isReplaying,
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit)
            );

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount),
                () -> {
                    persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit);
                    assertTrue(persistentSubscription.isReplaying());
                });

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertEquals(0, listener.liveJoinedCount);
            assertTrue(persistentSubscription.isReplaying());

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(5, fragmentHandler.receivedPayloads.size());

            final List<byte[]> liveMessages = generateRandomPayloads(15);
            persistentPublication.persist(liveMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount),
                () ->
                {
                    persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit);

                    // expect remaining messages to be consumed on the live channel
                    assertTrue(persistentSubscription.isLive());
                });

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
            assertEquals(0, listener.liveLeftCount);
            assertEquals(0, persistentSubscription.joinError());
            assertPayloads(fragmentHandler.receivedPayloads, replayMessages, liveMessages);

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10 })
    @InterruptAfter(5)
    void shouldCatchupOnReplayBeforeSwitchingToLive(final int fragmentLimit)
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final List<byte[]> payloads = generateRandomPayloads(5);
        persistentPublication.persist(payloads);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            final List<byte[]> payloads2 = generateRandomPayloads(25);
            persistentPublication.persist(payloads2);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size() + payloads2.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit));

            assertEquals(1, listener.liveJoinedCount);

            assertEquals(payloads.size() + payloads2.size(), fragmentHandler.receivedPayloads.size());

            // send some more messages
            final List<byte[]> payloads3 = generateRandomPayloads(5);
            persistentPublication.persist(payloads3);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(payloads.size() + payloads2.size() + payloads3.size()),
                () ->
                {
                    persistentSubscription.controlledPoll(fragmentHandler, fragmentLimit);

                    // expect remaining messages to be consumed on live channel
                    assertTrue(persistentSubscription.isLive());
                });

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
//          assertThat(persistentSubscription.joinError(), greaterThan(0L)); // TODO this test does not guarantee this.
            assertPayloads(fragmentHandler.receivedPayloads, payloads, payloads2, payloads3);

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldHandleReplayBeingAheadOfLive()
    {
        final String pubChannel = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:2000|rcv-wnd=4k";

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, pubChannel, STREAM_ID);

        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2);
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final Subscription subscription = aeron2.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(subscription);

        persistentPublication.persist(generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(32),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            executeUntil(
                () -> persistentPublication.receiverCount() == 2,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            executeUntil(persistentSubscription::isLive,
                () ->
                {
                    persistentSubscription.controlledPoll(fragmentHandler, 10);
                    subscription.poll((b, o, l, h) -> {}, 10);
                });

            assertEquals(-28 * 1024L, persistentSubscription.joinError());
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldSwitchFromReplayToLive()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(firstMessageBatch);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            assertEquals(0, listener.liveJoinedCount);

            // Start consuming messages over replay
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, archive.context().replaySessionCounter().get());
            assertTrue(persistentSubscription.isReplaying());

            // Continue consuming until we switch to live
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);

            assertTrue(fragmentHandler.hasReceivedPayloads(firstMessageBatch.size()));
            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);

            // Consume more messages on live
            final List<byte[]> secondMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(secondMessageBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(firstMessageBatch.size() + secondMessageBatch.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isLive());

            // Publish more messages and consume them from a 'faster' consumer, forcing the Persistent Subscription
            // to fall behind and drop off live.
            final MediaDriver.Context ctx = driverCtxTpl.clone()
                .aeronDirectoryName(CommonContext.generateRandomDirName());
            try (MediaDriver mediaDriver = MediaDriver.launch(ctx);
                Aeron aeron = Aeron.connect(
                    new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {
                final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
                final Subscription fastConsumer = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID);

                Tests.awaitConnected(fastConsumer);

                final List<byte[]> thirdMessageBatch = generateFixedPayloads(64, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(thirdMessageBatch);

                executeUntil(
                    () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(thirdMessageBatch.size()),
                    () -> fastConsumer.poll(fastSubscriptionFragmentHandler, 1)
                );

                // Verify the Persistent Subscription drops back to replay
                executeUntil(
                    persistentSubscription::isReplaying,
                    () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
                );
                assertTrue(persistentSubscription.isReplaying());

                assertEquals(1, listener.liveLeftCount);

                // Consume more messages until the Persistent Subscription rejoins live.
                final List<byte[]> fourthMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(fourthMessageBatch);

                final int expectedMessageCount = firstMessageBatch.size() +
                    secondMessageBatch.size() + thirdMessageBatch.size() + fourthMessageBatch.size();

                executeUntil(
                    () -> fragmentHandler.hasReceivedPayloads(expectedMessageCount) && persistentSubscription.isLive(),
                    () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

                assertEquals(2, listener.liveJoinedCount);

                assertPayloads(
                    fragmentHandler.receivedPayloads,
                    firstMessageBatch,
                    secondMessageBatch,
                    thirdMessageBatch,
                    fourthMessageBatch
                );
            }
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldJoinFromLiveWhenThereIsNoDataToReplay()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            final List<byte[]> messages = generateRandomPayloads(5);
            persistentPublication.persist(messages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(messages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            Tests.await(() -> archive.context().replaySessionCounter().get() == 0);
        }
    }

    @ParameterizedTest
    @InterruptAfter(15)
    @ValueSource(strings = { UNICAST_CHANNEL, IPC_CHANNEL })
    void anUntetheredPersistentSubscriptionCanFallBackToReplay(final String channel)
    {
        final ChannelUriStringBuilder channelUriStringBuilder = new ChannelUriStringBuilder(channel);

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, channel, STREAM_ID);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .liveChannel(channelUriStringBuilder.tether(false).build());

        final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
            Subscription fastSubscription = aeron.addSubscription(channelUriStringBuilder.tether(true)
                .build(), STREAM_ID))
        {
            Tests.awaitConnected(fastSubscription);

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );
            persistentPublication.persist(generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE));
            executeUntil(
                () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(32),
                () -> fastSubscription.poll(fastSubscriptionFragmentHandler, 10)
            );
            persistentPublication.persist(generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE));
            executeUntil(
                () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(64),
                () -> fastSubscription.poll(fastSubscriptionFragmentHandler, 10)
            );

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(64),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );

            assertTrue(persistentSubscription.isReplaying());

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldAssembleMessages()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, IPC_CHANNEL, STREAM_ID);

        final int sizeRequiringFragmentation = persistentPublication.maxPayloadLength() + 1;
        final byte[] payload0 = new byte[sizeRequiringFragmentation];
        ThreadLocalRandom.current().nextBytes(payload0);
        final byte[] payload1 = new byte[sizeRequiringFragmentation];
        ThreadLocalRandom.current().nextBytes(payload1);

        persistentPublication.persist(List.of(payload0));

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            persistentPublication.persist(List.of(payload1));

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(2),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertPayloads(fragmentHandler.receivedPayloads, List.of(payload0, payload1));
        }
    }

    @Test
    @InterruptAfter(10)
    void canFallbackToReplayInTheMiddleOfAFragmentedMessage()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final int maxPayloadLength = persistentPublication.maxPayloadLength();
        final byte[] firstHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(firstHalfOfMessage, (byte)1);
        final byte[] secondHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(secondHalfOfMessage, (byte)2);
        final byte[] largeMessage = new byte[firstHalfOfMessage.length + secondHalfOfMessage.length];
        System.arraycopy(firstHalfOfMessage, 0, largeMessage, 0, firstHalfOfMessage.length);
        System.arraycopy(secondHalfOfMessage, 0, largeMessage, firstHalfOfMessage.length, secondHalfOfMessage.length);

        final StreamIdFrameDataLossGenerator streamIdFrameDataLossGenerator = new StreamIdFrameDataLossGenerator();

        final String aeron2Dir = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtxWithLoss = driverCtxTpl.clone()
            .aeronDirectoryName(aeron2Dir)
            .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1))
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(streamIdFrameDataLossGenerator));
        addCloseable(TestMediaDriver.launch(driverCtxWithLoss, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driverCtxWithLoss.aeronDirectory());

        final Aeron.Context aeron2Context = aeronCtxTpl.clone()
            .aeronDirectoryName(aeron2Dir);
        final Aeron aeron2 = addCloseable(Aeron.connect(aeron2Context));

        persistentSubscriptionCtx
            .aeron(aeron2)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .liveStreamId(STREAM_ID)
            .recordingId(persistentPublication.recordingId())
            .aeronDirectoryName(aeron2Dir)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );

            final AtomicBoolean keepDroppingAfterMatch = new AtomicBoolean(false);
            streamIdFrameDataLossGenerator.enable(STREAM_ID,
                (bytes) ->
                {
                    final byte[] payload = new byte[bytes.length - DataHeaderFlyweight.HEADER_LENGTH];
                    System.arraycopy(bytes, DataHeaderFlyweight.HEADER_LENGTH, payload, 0, payload.length);
                    if (Arrays.equals(payload, secondHalfOfMessage))
                    {
                        keepDroppingAfterMatch.set(true);
                    }
                    return keepDroppingAfterMatch.get();
                }
            );
            persistentPublication.persist(List.of(largeMessage));

            executeUntil(
                persistentSubscription::isReplaying,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );
            assertTrue(fragmentHandler.receivedPayloads.isEmpty());

            streamIdFrameDataLossGenerator.disable();

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );
            assertPayloads(fragmentHandler.receivedPayloads, List.of(largeMessage));
        }
    }

    @Test
    @InterruptAfter(10)
    void canJoinLiveInTheMiddleOfAFragmentedMessage()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final int maxPayloadLength = driver.context().mtuLength() - DataHeaderFlyweight.HEADER_LENGTH;
        final byte[] firstHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(firstHalfOfMessage, (byte)1);
        final byte[] secondHalfOfMessage = new byte[maxPayloadLength];
        Arrays.fill(secondHalfOfMessage, (byte)2);
        final byte[] largeMessage = new byte[firstHalfOfMessage.length + secondHalfOfMessage.length];
        System.arraycopy(firstHalfOfMessage, 0, largeMessage, 0, firstHalfOfMessage.length);
        System.arraycopy(secondHalfOfMessage, 0, largeMessage, firstHalfOfMessage.length, secondHalfOfMessage.length);


        final String aeron2Dir = CommonContext.generateRandomDirName();
        final FrameDataLossGenerator frameDataLossGenerator = new FrameDataLossGenerator();

        final MediaDriver.Context driverCtxWithLoss = driverCtxTpl.clone()
            .aeronDirectoryName(aeron2Dir)
            .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1))
            .sendChannelEndpointSupplier(sendChannelEndpointSupplier(frameDataLossGenerator));

        addCloseable(TestMediaDriver.launch(driverCtxWithLoss, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driverCtxWithLoss.aeronDirectory());

        final Aeron.Context aeron2Context = aeronCtxTpl.clone().aeronDirectoryName(aeron2Dir);
        final Aeron aeron2 = addCloseable(Aeron.connect(aeron2Context));

        final ExclusivePublication exclusivePublication = aeron2.addExclusivePublication(MDC_PUBLICATION_CHANNEL, STREAM_ID);
        aeronArchive.startRecording(MDC_PUBLICATION_CHANNEL, STREAM_ID, SourceLocation.REMOTE);
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, exclusivePublication);

        AtomicBoolean keepDroppingAfterMatch = new AtomicBoolean(false);

        frameDataLossGenerator.enable(
            (bytes) ->
            {
                final byte[] payload = new byte[bytes.length - DataHeaderFlyweight.HEADER_LENGTH];
                System.arraycopy(bytes, DataHeaderFlyweight.HEADER_LENGTH, payload, 0, payload.length);
                if (Arrays.equals(payload, secondHalfOfMessage))
                {
                    keepDroppingAfterMatch.set(true);
                }
                return keepDroppingAfterMatch.get();
            }
        );
        persistentPublication.publish(List.of(largeMessage));

        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .liveStreamId(STREAM_ID)
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_START);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isReplaying,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(0, fragmentHandler.receivedPayloads.size());

            frameDataLossGenerator.disable();

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(1),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertPayloads(fragmentHandler.receivedPayloads, List.of(largeMessage));
        }
    }

    @Test
    @InterruptAfter(60)
    void canJoinLiveWhenLiveAndReplayAreAdvancing() throws Exception
    {
        final String pubChannel = "aeron:udp?term-length=16m|control=localhost:24325|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:24325|group=true";

        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2);
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, pubChannel, STREAM_ID);
        final Subscription controlSubscription = aeron.addSubscription(subChannel, STREAM_ID);
        Tests.awaitConnected(controlSubscription);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel)
            .listener(null);

        final int maxSeconds = 60;
        final int ratePerSecond = 10_000;
        final long maxProcessingTime = 1_000_000_000 / ratePerSecond / 8;
        final long t0 = System.nanoTime();
        final PerSecondStats publisherMessagesPerSecond = new PerSecondStats(t0, maxSeconds);
        final PerSecondStats publisherBpePerSecond = new PerSecondStats(t0, maxSeconds);
        final PerSecondStats controlMessagesPerSecond = new PerSecondStats(t0, maxSeconds);

        final Thread control = new Thread(
            () ->
            {
                final FragmentAssembler handler = new FragmentAssembler(
                    (buffer, offset, length, header) ->
                    {
                        controlMessagesPerSecond.record(System.nanoTime());
                        simulateWork(maxProcessingTime);
                    });
                while (!Thread.currentThread().isInterrupted())
                {
                    controlSubscription.poll(handler, 10);
                }
            },
            "testLiveJoinControl");
        control.start();
        addCloseable(() -> interruptAndJoin(control));

        final Thread publisher = new Thread(
            () ->
            {
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[2048]);
                long messageId = 0;
                long nextMessageAt = System.nanoTime() + exponentialArrivalDelay(ratePerSecond);
                while (!Thread.currentThread().isInterrupted())
                {
                    final long now = System.nanoTime();
                    if (now - nextMessageAt >= 0)
                    {
                        final int length = random.nextInt(2 * SIZE_OF_LONG, buffer.capacity() + 1);
                        buffer.putLong(0, messageId);
                        buffer.putLong(length - SIZE_OF_LONG, messageId);
                        final long result = persistentPublication.offer(buffer, 0, length);
                        if (result > 0)
                        {
                            messageId++;
                            nextMessageAt = now + exponentialArrivalDelay(ratePerSecond);
                            publisherMessagesPerSecond.record(now);
                        }
                        else if (result == BACK_PRESSURED)
                        {
                            publisherBpePerSecond.record(now);
                        }
                    }
                }
            },
            "testLiveJoinPublisher");
        publisher.start();
        addCloseable(() -> interruptAndJoin(publisher));

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final MessageVerifier handler = new MessageVerifier(maxProcessingTime);

            while (!persistentSubscription.isLive())
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to transition to live");
                }
            }

            interruptAndJoin(publisher);
            final long lastPosition = persistentPublication.position();

            while (handler.position < lastPosition)
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }

            interruptAndJoin(control);

            printResults(t0, persistentSubscription, ratePerSecond, publisherMessagesPerSecond,
                publisherBpePerSecond, controlMessagesPerSecond);
        }
    }

    @InterruptAfter(10)
    @Test
    void shouldStayOnAReplayWhenLiveCannotConnect()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> messages = generateRandomPayloads(5);
        persistentPublication.persist(messages);

        final String incorrectLiveChannel = "aeron:udp?control=localhost:49582|control-mode=dynamic|fc=max";

        aeronArchiveContext.messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_START)
            .liveChannel(incorrectLiveChannel);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(5),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isReplaying());
            Tests.await(() -> archive.context().replaySessionCounter().get() == 1);

            executeUntil(
                () -> listener.errorCount > 0,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );

            assertTrue(persistentSubscription.isReplaying());

            final List<byte[]> moreMessages = generateRandomPayloads(3);
            persistentPublication.persist(moreMessages);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(messages.size() + moreMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            assertTrue(persistentSubscription.isReplaying());
            Tests.await(() -> archive.context().replaySessionCounter().get() == 1);

            assertPayloads(fragmentHandler.receivedPayloads, messages, moreMessages);
        }
    }

    @InterruptAfter(10)
    @ParameterizedTest
    @ValueSource(longs = { FROM_START, FROM_LIVE })
    void shouldConnectToArchiveWhenItBecomesAvailable(final long startPosition, @TempDir Path tempDir )
    {
        this.archive.close();
        final File archiveDir = new File(tempDir.toString(), "testLocalArchive");
        final Archive.Context archiveCtx = this.archiveCtx.clone()
            .archiveDir(archiveDir)
            .deleteArchiveOnStart(false);
        final Archive archive = addCloseable(Archive.launch(archiveCtx.clone()));

        final AeronArchive aeronArchive = addCloseable(AeronArchive.connect(aeronArchiveContext.clone()));
        assert aeronArchive != null;

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        final List<byte[]> payloads = generateRandomPayloads(1);
        persistentPublication.persist(payloads);

        archive.close();

        aeronArchiveContext.messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(startPosition)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> listener.errorCount > 1, () -> persistentSubscription.controlledPoll(fragmentHandler, 1));
            assertEquals(TimeoutException.class, listener.lastException.getClass());
            addCloseable(Archive.launch(archiveCtx.clone()));
            executeUntil(persistentSubscription::isLive, () -> persistentSubscription.controlledPoll(fragmentHandler, 1));
        }
    }


    @InterruptAfter(5)
    @Test
    void shouldFailWhenLivePublicationIsRevoked()
    {
        final ExclusivePublication publication = addCloseable(aeron.addExclusivePublication(MDC_PUBLICATION_CHANNEL, STREAM_ID));
        aeronArchive.startRecording(MDC_PUBLICATION_CHANNEL, STREAM_ID, SourceLocation.LOCAL);

        final PersistentPublication persistentPublication = PersistentPublication.create(aeronArchive, publication);
        final List<byte[]> payloads = generateFixedPayloads(2, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(payloads);

        final Subscription subscription = addCloseable(aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID));

        persistentSubscriptionCtx
            .aeronArchiveContext(aeronArchiveContext.clone()
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(3)))
            .recordingId(persistentPublication.recordingId)
            .startPosition(FROM_START)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive, () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            Tests.await(subscription::isConnected);
            Tests.await(() -> subscription.imageCount() > 0);
            publication.revoke();
            Tests.await(publication::isClosed);
            Tests.await(() -> subscription.imageCount() == 0);
            subscription.close();

            executeUntil(() -> listener.errorCount > 0, () -> persistentSubscription.controlledPoll(fragmentHandler, 1));
            assertEquals(ArchiveException.class, listener.lastException.getClass());
            assertTrue(persistentSubscription.hasFailed());
        }
    }


    @Test
    @InterruptAfter(5)
    void canJoinLiveUponReachingEndOfRecordingIfLiveHasNotAdvanced()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> oldMessages = generateFixedPayloads(8, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(oldMessages);

        aeronArchive.stopRecording(persistentPublication.publication);

        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size()), () ->
                persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, oldMessages);

            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            final List<byte[]> newMessages = generateFixedPayloads(16, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size() + newMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldJoinLiveUponReachingEndOfRecordingWhenLiveBecomesAvailable()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> oldMessages = generateFixedPayloads(8, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(oldMessages);

        persistentPublication.close();
        Tests.await(() -> !persistentPublication.publicationCountersExist());

        aeronArchiveContext.messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        final long recordingId = persistentPublication.recordingId();
        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(recordingId);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Ensure we can still consume from replay
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size()), () ->
                persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertPayloads(fragmentHandler.receivedPayloads, oldMessages);

            executeUntil(
                () -> listener.errorCount > 0,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );
            assertThat(
                listener.lastException.getMessage(),
                containsString("No image became available on the live subscription")
            );

            // Restart the live publication
            final PersistentPublication resumedPublication = PersistentPublication.resume(
                aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID, recordingId
            );

            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            final List<byte[]> newMessages = generateFixedPayloads(16, ONE_KB_MESSAGE_SIZE);
            resumedPublication.persist(newMessages);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(oldMessages.size() + newMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertTrue(persistentSubscription.isLive());
            assertFalse(persistentSubscription.isReplaying());
            assertPayloads(fragmentHandler.receivedPayloads, oldMessages, newMessages);
        }
    }

    @Test
    @InterruptAfter(5)
    void cannotJoinLiveUponReachingEndOfRecordingIfLiveHasAdvanced()
    {
        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> recordedMessages = generateFixedPayloads(8, ONE_KB_MESSAGE_SIZE);
        persistentPublication.persist(recordedMessages);

        aeronArchive.stopRecording(persistentPublication.publication);

        final List<byte[]> messagesAfterRecording = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);
        persistentPublication.publish(messagesAfterRecording);

        // Add another consumer to allow the live position to advance
        try (final Subscription subscriber2 = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID))
        {
            final BufferingFragmentHandler subscriber2FragmentHandler = new BufferingFragmentHandler();
            executeUntil(
                () -> subscriber2FragmentHandler.hasReceivedPayloads(messagesAfterRecording.size()),
                () -> subscriber2.controlledPoll(subscriber2FragmentHandler, 1)
            );
        }

        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId());

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(() -> fragmentHandler.hasReceivedPayloads(recordedMessages.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

            assertTrue(persistentSubscription.isReplaying());

            executeUntil(persistentSubscription::hasFailed,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(recordedMessages.size(), fragmentHandler.receivedPayloads.size());
            assertEquals(1, listener.errorCount);
            assertEquals(ArchiveException.class, listener.lastException.getClass());
            assertThat(listener.lastException.getMessage(), containsString(
                "ERROR - replay request failed")
            );
        }
    }

    @InterruptAfter(10)
    @Test
    void canFallbackToReplayAfterStartingFromLive()
    {

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final List<byte[]> firstMessageBatch = generateRandomPayloads(2);
        persistentPublication.persist(firstMessageBatch);

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId())
            .startPosition(FROM_LIVE)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            // Start consuming messages from live
            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1)
            );
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, archive.context().replaySessionCounter().get());
            assertFalse(persistentSubscription.isReplaying());

            final List<byte[]> secondMessageBatch = generateRandomPayloads(5);
            persistentPublication.persist(secondMessageBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(secondMessageBatch.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            // Publish more messages and consume them from a 'faster' consumer, forcing the Persistent Subscription
            // to fall behind and drop off live.
            final MediaDriver.Context ctx = driverCtxTpl.clone()
                .aeronDirectoryName(CommonContext.generateRandomDirName());
            try (MediaDriver mediaDriver = MediaDriver.launch(ctx);
                Aeron aeron = Aeron.connect(
                    new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {
                final CountingFragmentHandler fastSubscriptionFragmentHandler = new CountingFragmentHandler();
                final Subscription fastConsumer = aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID);

                Tests.awaitConnected(fastConsumer);

                final List<byte[]> thirdMessageBatch = generateFixedPayloads(64, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(thirdMessageBatch);

                executeUntil(
                    () -> fastSubscriptionFragmentHandler.hasReceivedPayloads(thirdMessageBatch.size()),
                    () -> fastConsumer.poll(fastSubscriptionFragmentHandler, 1)
                );

                // Verify the Persistent Subscription drops back to replay
                executeUntil(
                    persistentSubscription::isReplaying,
                    () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
                );
                assertTrue(persistentSubscription.isReplaying());

                assertEquals(1, listener.liveLeftCount);

                // Consume more messages until the Persistent Subscription rejoins live.
                final List<byte[]> fourthMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
                persistentPublication.persist(fourthMessageBatch);

                final int expectedMessageCount = secondMessageBatch.size() + thirdMessageBatch.size() +
                    fourthMessageBatch.size();

                executeUntil(
                    () -> fragmentHandler.hasReceivedPayloads(expectedMessageCount) && persistentSubscription.isLive(),
                    () -> persistentSubscription.controlledPoll(fragmentHandler, 10));

                assertEquals(2, listener.liveJoinedCount);

                assertPayloads(
                    fragmentHandler.receivedPayloads,
                    secondMessageBatch,
                    thirdMessageBatch,
                    fourthMessageBatch
                );
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void cannotFallbackToReplayWhenTheRecordingHasStoppedAtAnEarlierPosition()
    {
        PersistentPublication persistentPublication = PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final MediaDriver.Context ctx = driverCtxTpl.clone()
            .aeronDirectoryName(CommonContext.generateRandomDirName());
        MediaDriver mediaDriver = addCloseable(MediaDriver.launch(ctx));
        Aeron aeron = addCloseable(
            Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())));

        final Subscription fastSubscription = addCloseable(aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID));
        final CountingFragmentHandler countingFragmentHandler = new CountingFragmentHandler();

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive, () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            // Consume a batch of messages on live
            final List<byte[]> firstMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(firstMessageBatch);
            executeUntil(
                () -> countingFragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastSubscription.poll(countingFragmentHandler, 10));

            // Stop the recording
            aeronArchive.stopRecording(persistentPublication.publication);

            // Consume more messages on live, which will not be recorded
            final List<byte[]> secondMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(secondMessageBatch);

            executeUntil(
                () -> fragmentHandler.hasReceivedPayloads(firstMessageBatch.size() + secondMessageBatch.size()),
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );

            // Allow a faster consumer to advance ahead, causing the persistent subscription to drop from live.
            final List<byte[]> thirdMessageBatch = generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(thirdMessageBatch);
            executeUntil(
                () -> countingFragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastSubscription.poll(countingFragmentHandler, 10)
            );

            final List<byte[]> fourthMessageBatch = generateFixedPayloads(33, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(fourthMessageBatch);
            executeUntil(
                () -> countingFragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastSubscription.poll(countingFragmentHandler, 10)
            );

            // Verify we cannot fall back to replay, as we have already advanced beyond the stop position of the recording.
            executeUntil(
                persistentSubscription::hasFailed,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );
            assertEquals(ArchiveException.class, listener.lastException.getClass());
            assertThat(
                listener.lastException.getMessage(),
                containsString("must be less than highest recorded position")
            );
        }
    }

    @Test
    @InterruptAfter(10)
    void cannotFallbackToReplayWhenTheRecordingHasBeenRemoved()
    {
        PersistentPublication persistentPublication = PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);

        final MediaDriver.Context ctx = driverCtxTpl.clone()
            .aeronDirectoryName(CommonContext.generateRandomDirName());
        MediaDriver mediaDriver = addCloseable(MediaDriver.launch(ctx));
        Aeron aeron = addCloseable(
            Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())));

        final Subscription fastSubscription = addCloseable(aeron.addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID));
        final CountingFragmentHandler countingFragmentHandler = new CountingFragmentHandler();

        persistentSubscriptionCtx
            .recordingId(persistentPublication.recordingId)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive, () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            // Consume a batch of messages on live
            final List<byte[]> firstMessageBatch = generateFixedPayloads(5, ONE_KB_MESSAGE_SIZE);
            persistentPublication.persist(firstMessageBatch);
            executeUntil(
                () -> countingFragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastSubscription.poll(countingFragmentHandler, 10));

            // Remove the recording
            aeronArchive.stopRecording(persistentPublication.publication);
            aeronArchive.purgeRecording(persistentPublication.recordingId);

            // Allow a faster consumer to advance ahead, causing the persistent subscription to drop from live.
            final List<byte[]> secondMessageBatch = generateFixedPayloads(32, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(secondMessageBatch);
            executeUntil(
                () -> countingFragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastSubscription.poll(countingFragmentHandler, 10)
            );

            final List<byte[]> thirdMessageBatch = generateFixedPayloads(33, ONE_KB_MESSAGE_SIZE);
            persistentPublication.publish(thirdMessageBatch);
            executeUntil(
                () -> countingFragmentHandler.hasReceivedPayloads(persistentPublication.publishedMessageCount()),
                () -> fastSubscription.poll(countingFragmentHandler, 10)
            );

            // Verify we cannot fall back to replay, as the recording no longer exists
            executeUntil(
                persistentSubscription::hasFailed,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );
            assertEquals(ArchiveException.class, listener.lastException.getClass());
            assertThat(
                listener.lastException.getMessage(),
                containsString("unknown recording id:")
            );
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldRecoverFromReplayChannelNetworkProblems() throws Exception
    {
        shouldRecoverFromNetworkProblems(NetworkFlow.REPLAY);
    }

    @Test
    @InterruptAfter(20)
    void shouldRecoverFromLiveChannelNetworkProblems() throws Exception
    {
        shouldRecoverFromNetworkProblems(NetworkFlow.LIVE);
    }

    private enum NetworkFlow
    {
        REPLAY,
        LIVE,
    }

    private void shouldRecoverFromNetworkProblems(final NetworkFlow victimFlow) throws Exception
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final String pubChannel = "aeron:udp?term-length=16m|control=localhost:24325|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:24325|group=true";

        final StreamIdLossGenerator lossGenerator = new StreamIdLossGenerator();
        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2)
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(lossGenerator));
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, pubChannel, STREAM_ID);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel);

        final int ratePerSecond = 10_000;
        final long maxProcessingTime = 1_000_000_000 / ratePerSecond / 8;

        final Thread publisher = new Thread(
            () ->
            {
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[2048]);
                long messageId = 0;
                long nextMessageAt = System.nanoTime() + exponentialArrivalDelay(ratePerSecond);
                while (!Thread.currentThread().isInterrupted())
                {
                    final long now = System.nanoTime();
                    if (now - nextMessageAt >= 0)
                    {
                        final int length = random.nextInt(2 * SIZE_OF_LONG, buffer.capacity() + 1);
                        buffer.putLong(0, messageId);
                        buffer.putLong(length - SIZE_OF_LONG, messageId);
                        final long result = persistentPublication.offer(buffer, 0, length);
                        if (result > 0)
                        {
                            messageId++;
                            nextMessageAt = now + exponentialArrivalDelay(ratePerSecond);
                        }
                    }
                }
            },
            "shouldRecoverFromNetworkProblemsPublisher");
        publisher.start();
        addCloseable(() -> interruptAndJoin(publisher));

        final long startTime = System.nanoTime();

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final MessageVerifier handler = new MessageVerifier(maxProcessingTime);

            enum LossState
            {
                NOT_STARTED,
                WAITING_TO_START,
                IN_PROGRESS,
                FINISHED,
            }

            LossState lossState = LossState.NOT_STARTED;
            long deadline = 0;

            while (!persistentSubscription.isLive())
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to transition to live");
                }

                if (victimFlow == NetworkFlow.REPLAY)
                {
                    if (lossState == LossState.NOT_STARTED && persistentSubscription.isReplaying())
                    {
                        lossState = LossState.WAITING_TO_START;
                        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
                    }

                    if (lossState == LossState.WAITING_TO_START && System.nanoTime() - deadline >= 0)
                    {
                        lossState = LossState.IN_PROGRESS;
                        deadline = System.nanoTime() +
                            driver2Ctx.imageLivenessTimeoutNs() + TimeUnit.MILLISECONDS.toNanos(200);
                        lossGenerator.enable(persistentSubscriptionCtx.replayStreamId());
                    }

                    if (lossState == LossState.IN_PROGRESS && System.nanoTime() - deadline >= 0)
                    {
                        lossState = LossState.FINISHED;
                        lossGenerator.disable();
                    }
                }
            }

            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);

            if (victimFlow == NetworkFlow.LIVE)
            {
                lossState = LossState.WAITING_TO_START;
                deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);

                while (true)
                {
                    if (persistentSubscription.controlledPoll(handler, 10) == 0)
                    {
                        checkForInterrupt("interrupted while simulating live channel network problems");
                    }

                    if (lossState == LossState.WAITING_TO_START && System.nanoTime() - deadline >= 0)
                    {
                        lossState = LossState.IN_PROGRESS;
                        lossGenerator.enable(persistentSubscriptionCtx.liveStreamId());
                    }

                    if (lossState == LossState.IN_PROGRESS && !persistentSubscription.isLive())
                    {
                        assertEquals(1, listener.liveLeftCount);
                        lossState = LossState.FINISHED;
                        lossGenerator.disable();
                    }

                    if (lossState == LossState.FINISHED && persistentSubscription.isLive())
                    {
                        assertEquals(2, listener.liveJoinedCount);
                        break;
                    }
                }
            }

            interruptAndJoin(publisher);

            final long durationNs = System.nanoTime() - startTime;
            final long minExpectedPosition = TimeUnit.NANOSECONDS.toSeconds(durationNs) * ratePerSecond * 64L;
            final long lastPosition = persistentPublication.position();
            assertThat(lastPosition, greaterThanOrEqualTo(minExpectedPosition));

            while (handler.position < lastPosition)
            {
                if (persistentSubscription.controlledPoll(handler, 10) == 0)
                {
                    checkForInterrupt("failed to drain the stream");
                }
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRejoinLiveEvenIfNoFragmentsHaveBeenConsumedAfterJoiningFromLive()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("loss generator");

        final String pubChannel = "aeron:udp?term-length=16m|control=localhost:24325|control-mode=dynamic|fc=min";
        final String subChannel = "aeron:udp?control=localhost:24325|group=true";

        final StreamIdLossGenerator lossGenerator = new StreamIdLossGenerator();
        final String aeronDir2 = CommonContext.generateRandomDirName();
        final MediaDriver.Context driver2Ctx = driverCtxTpl.clone().aeronDirectoryName(aeronDir2)
            .receiveChannelEndpointSupplier(receiveChannelEndpointSupplier(lossGenerator));
        addCloseable(TestMediaDriver.launch(driver2Ctx, systemTestWatcher));
        systemTestWatcher.dataCollector().add(driver2Ctx.aeronDirectory());
        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeronDir2)));

        final PersistentPublication persistentPublication =
            PersistentPublication.create(aeronArchive, pubChannel, STREAM_ID);

        final List<byte[]> oldMessages = generateRandomPayloads(randomWatcher.random().nextInt(3));
        persistentPublication.persist(oldMessages);

        persistentSubscriptionCtx
            .aeron(aeron2)
            .recordingId(persistentPublication.recordingId())
            .liveChannel(subChannel)
            .startPosition(FROM_LIVE);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            final Runnable pollSubscription = () -> persistentSubscription.controlledPoll(fragmentHandler, 10);

            executeUntil(persistentSubscription::isLive, pollSubscription);

            assertEquals(0, fragmentHandler.receivedPayloads.size());
            assertEquals(1, listener.liveJoinedCount);
            assertEquals(0, listener.liveLeftCount);

            lossGenerator.enable(persistentSubscriptionCtx.liveStreamId());

            executeUntil(() -> !persistentSubscription.isLive(), pollSubscription);

            assertEquals(1, listener.liveJoinedCount);
            assertEquals(1, listener.liveLeftCount);

            lossGenerator.disable();

            executeUntil(persistentSubscription::isLive, pollSubscription);

            assertEquals(0, fragmentHandler.receivedPayloads.size());
            assertEquals(2, listener.liveJoinedCount);
            assertEquals(1, listener.liveLeftCount);

            final List<byte[]> payloads = generateRandomPayloads(3);
            persistentPublication.persist(payloads);

            executeUntil(() -> fragmentHandler.hasReceivedPayloads(payloads.size()), pollSubscription);

            assertPayloads(fragmentHandler.receivedPayloads, payloads);
        }
    }


    @Test
    @InterruptAfter(10)
    void shouldReconnectToTheArchiveAfterArchiveRestart(@TempDir Path tempDir)
    {
        final String aeron2Dir = CommonContext.generateRandomDirName();

        final MediaDriver.Context driver2CtxTpl = driverCtxTpl.clone()
            .aeronDirectoryName(aeron2Dir)
            .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(2));

        final TestMediaDriver mediaDriver2 = TestMediaDriver.launch(driver2CtxTpl.clone(), systemTestWatcher);
        addCloseable(mediaDriver2);
        systemTestWatcher.dataCollector().add(driver2CtxTpl.aeronDirectory());

        final Aeron aeron2 = addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeron2Dir)));

        final String archiveControlRequestChannel = "aeron:udp?endpoint=localhost:8011";
        final File remoteArchiveDir = new File(tempDir.toString(), "remoteArchiveDir");

        final Archive.Context remoteArchiveCtx = archiveCtx.clone()
            .archiveDir(remoteArchiveDir)
            .aeronDirectoryName(aeron2Dir)
            .controlChannel(archiveControlRequestChannel)
            .deleteArchiveOnStart(false);

        final Archive archive = Archive.launch(remoteArchiveCtx.clone());
        addCloseable(archive);
        systemTestWatcher.dataCollector().add(remoteArchiveCtx.archiveDir());

        final AeronArchive.Context remoteAeronArchiveContext = TestContexts.localhostAeronArchive()
            .controlRequestChannel(archiveControlRequestChannel)
            .aeron(aeron2);

        final AeronArchive remoteArchive = AeronArchive.connect(remoteAeronArchiveContext.clone());
        addCloseable(remoteArchive);
        assert remoteArchive != null;

        final ExclusivePublication exclusivePublication = addCloseable(aeron.addExclusivePublication(MDC_PUBLICATION_CHANNEL, STREAM_ID));
        remoteArchive.startRecording(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID, SourceLocation.REMOTE);
        Tests.awaitConnected(exclusivePublication);

        final PersistentPublication persistentPublication =
            PersistentPublication.create(remoteArchive, exclusivePublication);

        persistentSubscriptionCtx
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .recordingId(persistentPublication.recordingId())
            .aeronArchiveContext(remoteAeronArchiveContext)
            .startPosition(FROM_START);

        final List<byte[]> payloads64 = generateFixedPayloads(64, ONE_KB_MESSAGE_SIZE);
        final List<byte[]> payloads1 = generateFixedPayloads(1, ONE_KB_MESSAGE_SIZE);

        persistentPublication.persist(payloads1);

        try (PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx))
        {
            executeUntil(persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 1));

            assertEquals(1, fragmentHandler.receivedPayloads.size());

            persistentPublication.persist(payloads64);
            persistentPublication.persist(payloads1);

            archive.close();
            aeron2.close();
            mediaDriver2.close();
            addCloseable(TestMediaDriver.launch(driver2CtxTpl.clone(), systemTestWatcher));

            addCloseable(Aeron.connect(aeronCtxTpl.clone().aeronDirectoryName(aeron2Dir)));
            addCloseable(Archive.launch(remoteArchiveCtx.clone()));

            executeUntil(
                persistentSubscription::isReplaying,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );
            executeUntil(
                persistentSubscription::isLive,
                () -> persistentSubscription.controlledPoll(fragmentHandler, 10)
            );
            assertPayloads(fragmentHandler.receivedPayloads, payloads1, payloads64, payloads1);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldCreateOwnAeronInstanceWhenNotSupplied()
    {
        final PersistentPublication persistentPublication = PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        persistentPublication.persist(generateRandomPayloads(2));

        PersistentSubscription.Context persistentSubscriptionCtx = new PersistentSubscription.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .recordingId(persistentPublication.recordingId)
            .startPosition(FROM_START)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .liveStreamId(STREAM_ID)
            .replayChannel("aeron:udp?endpoint=localhost:0")
            .replayStreamId(-5)
            .listener(listener)
            .aeronArchiveContext(aeronArchiveContext);

        final PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
        executeUntil(
            persistentSubscription::isLive,
            () -> persistentSubscription.controlledPoll(fragmentHandler, 1));
        final Aeron aeron = persistentSubscriptionCtx.aeron();
        assertNotNull(aeron);
        persistentSubscription.close();
        assertTrue(aeron.isClosed());
    }

    @Test
    @InterruptAfter(5)
    void shouldNotCloseSuppliedAeronInstance()
    {
        final PersistentPublication persistentPublication = PersistentPublication.create(aeronArchive, MDC_PUBLICATION_CHANNEL, STREAM_ID);
        persistentPublication.persist(generateRandomPayloads(2));

        final PersistentSubscription.Context persistentSubscriptionCtx = this.persistentSubscriptionCtx.clone()
            .aeron(aeron)
            .recordingId(persistentPublication.recordingId)
            .startPosition(FROM_START)
            .liveChannel(MDC_SUBSCRIPTION_CHANNEL)
            .aeronArchiveContext(aeronArchiveContext);

        final PersistentSubscription persistentSubscription = PersistentSubscription.create(persistentSubscriptionCtx);
        executeUntil(
            persistentSubscription::isLive,
            () -> persistentSubscription.controlledPoll(fragmentHandler, 1));
        persistentSubscription.close();
        assertFalse(aeron.isClosed());
    }

    private static ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier(final LossGenerator lossGenerator)
    {
        return (udpChannel, dispatcher, statusIndicator, context) ->
            new DebugReceiveChannelEndpoint(
                udpChannel, dispatcher, statusIndicator, context, lossGenerator, lossGenerator);
    }

    private static SendChannelEndpointSupplier sendChannelEndpointSupplier(final LossGenerator lossGenerator)
    {
        return (udpChannel, statusIndicator, context) ->
            new DebugSendChannelEndpoint(
                udpChannel, statusIndicator, context, lossGenerator, lossGenerator);
    }

    private static void interruptAndJoin(final Thread thread) throws InterruptedException
    {
        thread.interrupt();
        thread.join();
    }

    private static void checkForInterrupt(final String message)
    {
        if (Thread.interrupted())
        {
            fail(message);
        }
    }

    private static void simulateWork(final long maxProcessingTime)
    {
        if (maxProcessingTime > 0)
        {
            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(maxProcessingTime));
        }
    }

    private static void printResults(
        final long t0,
        final PersistentSubscription persistentSubscription,
        final int ratePerSecond,
        final PerSecondStats publisherMessagesPerSecond,
        final PerSecondStats publisherBpePerSecond,
        final PerSecondStats controlMessagesPerSecond)
    {
        final int elapsedSeconds = (int)((System.nanoTime() - t0 + 999_999_999) / 1_000_000_000);

        System.out.println("join error = " + persistentSubscription.joinError());
        System.out.println("expected rate per second = " + ratePerSecond);
        System.out.println("second,published,publisherBpe,control");
        for (int i = 0; i < elapsedSeconds; i++)
        {
            System.out.println(i +
                "," + publisherMessagesPerSecond.get(i) +
                "," + publisherBpePerSecond.get(i) +
                "," + controlMessagesPerSecond.get(i));
        }
    }

    private static long exponentialArrivalDelay(final long ratePerSecond)
    {
        final double uniform = ThreadLocalRandom.current().nextDouble();
        final double secondFraction = -Math.log(1.0 - uniform) / ratePerSecond;
        return (long)(secondFraction * 1e9);
    }

    private static final class MessageVerifier implements ControlledFragmentHandler
    {
        private final long maxProcessingTime;
        long expectedMessageId;
        long position;

        private MessageVerifier(final long maxProcessingTime)
        {
            this.maxProcessingTime = maxProcessingTime;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (length < 2 * SIZE_OF_LONG)
            {
                throw new IllegalStateException("length was " + length);
            }
            final long messageId1 = buffer.getLong(offset);
            final long messageId2 = buffer.getLong(offset + length - SIZE_OF_LONG);
            if (messageId1 != messageId2)
            {
                throw new IllegalStateException("message had different ids " + messageId1 + " and " + messageId2);
            }
            if (messageId1 != expectedMessageId)
            {
                throw new IllegalStateException("expected id " + expectedMessageId + ", but got " + messageId1);
            }
            expectedMessageId = messageId1 + 1;
            position = header.position();
            simulateWork(maxProcessingTime);
            return Action.CONTINUE;
        }
    }

    private static final class PerSecondStats
    {
        private final long[] perSecond;
        private final long t0;

        PerSecondStats(final long t0, final int maxSeconds)
        {
            this.t0 = t0;
            this.perSecond = new long[maxSeconds];
        }

        void record(final long ts)
        {
            final int second = (int)((ts - t0) / 1_000_000_000);
            if (second < perSecond.length)
            {
                perSecond[second]++;
            }
        }

        long get(final int second)
        {
            return perSecond[second];
        }
    }

    private <T extends AutoCloseable> T addCloseable(final T closeable)
    {
        closeables.add(closeable);
        return closeable;
    }

    private void closeCloseables() throws Exception
    {
        Exception ex = null;

        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            final AutoCloseable closeable = closeables.get(i);
            try
            {
                closeable.close();
            }
            catch (final Exception e)
            {
                if (ex == null)
                {
                    ex = e;
                }
                else
                {
                    ex.addSuppressed(e);
                }
            }
        }

        if (ex != null)
        {
            throw ex;
        }
    }

    private List<byte[]> generateFixedPayloads(final int count, final int size)
    {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final List<byte[]> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            final byte[] payload = new byte[size];
            random.nextBytes(payload);
            payloads.add(payload);
        }
        return payloads;
    }

    private List<byte[]> generateRandomPayloads(final int count)
    {
        final Random random = randomWatcher.random();
        final List<byte[]> randomPayloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            final int length = random.nextInt(2048);
            final byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            randomPayloads.add(bytes);
        }
        return randomPayloads;
    }

    private static void executeUntil(final BooleanSupplier predicate, final Runnable runnable)
    {
        Tests.await(() ->
        {
            runnable.run();
            return predicate.getAsBoolean();
        });
    }

    private void assertPayloads(final List<byte[]> receivedPayloads, final List<byte[]> payloads)
    {
        assertEquals(payloads.size(), receivedPayloads.size());
        for (int i = 0; i < receivedPayloads.size(); i++)
        {
            assertArrayEquals(payloads.get(i), receivedPayloads.get(i));
        }
    }

    @SafeVarargs
    private void assertPayloads(final List<byte[]> receivedPayloads, final List<byte[]>... payloads)
    {
        final List<byte[]> allPayloads = new ArrayList<>();
        for (final List<byte[]> payload : payloads)
        {
            allPayloads.addAll(payload);
        }

        assertPayloads(receivedPayloads, allPayloads);
    }

    private static Stream<Arguments> liveChannels()
    {
        return Stream.of(1, 10, Integer.MAX_VALUE)
            .flatMap((fragmentLimit) -> Stream.of(
                arguments(fragmentLimit, IPC_CHANNEL, IPC_CHANNEL),
                arguments(fragmentLimit, MULTICAST_CHANNEL, MULTICAST_CHANNEL),
                arguments(fragmentLimit, SPY_PREFIX + MDC_PUBLICATION_CHANNEL, MDC_PUBLICATION_CHANNEL)
            ));
    }

    private static Stream<Arguments> replayChannelsAndStreams()
    {
        return Stream.of(
            arguments("aeron:udp?endpoint=localhost:0",
                -10,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                LOCALHOST_CONTROL_RESPONSE_CHANNEL
            ),
            arguments(
                "aeron:udp?endpoint=localhost:10001",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                LOCALHOST_CONTROL_RESPONSE_CHANNEL
            ),
            arguments("aeron:ipc", -12, LOCALHOST_CONTROL_REQUEST_CHANNEL, LOCALHOST_CONTROL_RESPONSE_CHANNEL),
            arguments(
                "aeron:udp?control=localhost:10001|control-mode=response",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(
                "aeron:udp?control=localhost:10001|control-mode=response|endpoint=localhost:5006",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(
                "aeron:udp?control=localhost:10001|control-mode=response|endpoint=localhost:0",
                -11,
                LOCALHOST_CONTROL_REQUEST_CHANNEL,
                "aeron:udp?control-mode=response|control=localhost:10002"),
            arguments("aeron:ipc?control-mode=response", -11, "aeron:ipc", "aeron:ipc?control-mode=response")
        );
    }

    private static String removeSessionId(final String channel)
    {
        final ChannelUri uri = ChannelUri.parse(channel);
        uri.remove(SESSION_ID_PARAM_NAME);
        return uri.toString();
    }

    private static final class BufferingFragmentHandler implements ControlledFragmentHandler
    {
        private final List<byte[]> receivedPayloads = new ArrayList<>();
        private long position;

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            position = header.position();
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            receivedPayloads.add(bytes);
            return Action.CONTINUE;
        }

        boolean hasReceivedPayloads(final int numberOfPayloads)
        {
            return receivedPayloads.size() >= numberOfPayloads;
        }
    }

    private static final class CountingFragmentHandler implements FragmentHandler
    {
        private long receivedFragments = 0;

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            receivedFragments++;
        }

        boolean hasReceivedPayloads(final int numberOfPayloads)
        {
            return receivedFragments >= numberOfPayloads;
        }
    }

    private static final class PersistentSubscriptionListenerImpl implements PersistentSubscriptionListener
    {
        long liveJoinedCount;
        long liveLeftCount;
        int errorCount;
        Exception lastException;

        public void onLiveJoined()
        {
            liveJoinedCount++;
        }

        public void onLiveLeft()
        {
            liveLeftCount++;
        }

        public void onError(final Exception e)
        {
            errorCount++;
            lastException = e;
        }
    }

    private static final class PersistentPublication implements AutoCloseable
    {
        private final AeronArchive aeronArchive;
        private final ExclusivePublication publication;
        private final long recordingId;
        private final CountersReader countersReader;
        private final int recordingCounterId;
        private int publishedMessageCount = 0;

        static PersistentPublication create(
            final AeronArchive aeronArchive,
            final String channel,
            final int streamId)
        {
            final ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId);
            final CountersReader countersReader = aeronArchive.context().aeron().countersReader();
            final int recordingCounterId =
                Tests.awaitRecordingCounterId(countersReader, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);

            return new PersistentPublication(
                aeronArchive,
                publication,
                recordingId,
                countersReader,
                recordingCounterId);
        }

        static PersistentPublication create(
            final AeronArchive aeronArchive,
            final ExclusivePublication publication
        )
        {
            final CountersReader countersReader = aeronArchive.context().aeron().countersReader();
            final int recordingCounterId =
                Tests.awaitRecordingCounterId(countersReader, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);

            return new PersistentPublication(
                aeronArchive,
                publication,
                recordingId,
                countersReader,
                recordingCounterId);
        }

        static PersistentPublication resume(
            final AeronArchive aeronArchive,
            final String channel,
            final int streamId,
            final long recordingId)
        {
            final ChannelUriStringBuilder channelUriBuilder = new ChannelUriStringBuilder(channel);
            aeronArchive.listRecording(recordingId,
                (controlSessionId,
                    correlationId,
                    recordingId1,
                    startTimestamp,
                    stopTimestamp,
                    startPosition,
                    stopPosition,
                    initialTermId,
                    segmentFileLength,
                    termBufferLength,
                    mtuLength,
                    sessionId,
                    streamId1,
                    strippedChannel,
                    originalChannel,
                    sourceIdentity) ->
                    channelUriBuilder.initialPosition(stopPosition, initialTermId, termBufferLength
                    )
            );

            final Aeron aeron = aeronArchive.context().aeron();
            final String channelUri = channelUriBuilder.build();
            final ExclusivePublication publication = aeron.addExclusivePublication(channelUri, streamId);
            aeronArchive.extendRecording(recordingId, channelUri, streamId, SourceLocation.LOCAL);
            final CountersReader countersReader = aeron.countersReader();

            final int recordingCounterId =
                Tests.awaitRecordingCounterId(countersReader, publication.sessionId(), aeronArchive.archiveId());
            final long recordingIdFromCounter = RecordingPos.getRecordingId(countersReader, recordingCounterId);
            assert recordingId == recordingIdFromCounter;

            return new PersistentPublication(
                aeronArchive,
                publication,
                recordingId,
                countersReader,
                recordingCounterId);
        }

        private PersistentPublication(
            final AeronArchive aeronArchive,
            final ExclusivePublication publication,
            final long recordingId,
            final CountersReader countersReader,
            final int recordingCounterId)
        {
            this.aeronArchive = aeronArchive;
            this.publication = publication;
            this.recordingId = recordingId;
            this.countersReader = countersReader;
            this.recordingCounterId = recordingCounterId;
        }

        long offer(final DirectBuffer buffer, final int offset, final int length)
        {
            final long result = publication.offer(buffer, offset, length);
            if (result > 0)
            {
                publishedMessageCount++;
            }
            return result;
        }

        void persist(final List<byte[]> messages)
        {
            if (messages.isEmpty())
            {
                return;
            }
            long position = publish(messages);
            Tests.awaitPosition(countersReader, recordingCounterId, position);
        }

        long publish(final List<byte[]> messages)
        {
            final UnsafeBuffer wrapper = new UnsafeBuffer();

            long position = publication.position();
            for (final byte[] message : messages)
            {
                wrapper.wrap(message);
                while ((position = publication.offer(wrapper)) < 0)
                {
                    Tests.yieldingIdle("failed to offer due to " + Publication.errorString(position));
                }
            }
            publishedMessageCount += messages.size();
            return position;
        }

        long stop()
        {
            aeronArchive.stopRecording(publication);
            return aeronArchive.getStopPosition(recordingId);
        }

        long receiverCount()
        {
            final int receiversCounterId = countersReader.findByTypeIdAndRegistrationId(
                FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID, publication.registrationId());
            assertNotEquals(NULL_COUNTER_ID, receiversCounterId);
            return countersReader.getCounterValue(receiversCounterId);
        }

        int publishedMessageCount()
        {
            return publishedMessageCount;
        }

        boolean publicationCountersExist()
        {
            final int receiversCounterId = countersReader.findByTypeIdAndRegistrationId(
                DRIVER_PUBLISHER_POS_TYPE_ID, publication.registrationId());
            return receiversCounterId != NULL_COUNTER_ID;
        }

        long recordingId()
        {
            return recordingId;
        }

        int maxPayloadLength()
        {
            return publication.maxPayloadLength();
        }

        long position()
        {
            return publication.position();
        }

        public void close()
        {
            aeronArchive.stopRecording(publication);
            CloseHelper.close(publication);
        }
    }
}
