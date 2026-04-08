/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
#ifndef AERON_ARCHIVE_WRAPPER_PERSISTENT_SUBSCRIPTION_H
#define AERON_ARCHIVE_WRAPPER_PERSISTENT_SUBSCRIPTION_H

#include "AeronArchive.h"
#include "client/aeron_archive_persistent_subscription.h"
extern "C"
{
#include "util/aeron_error.h"
}

namespace aeron { namespace archive { namespace client
{

/**
 * A subscription that automatically switches between consuming from a live stream and replaying
 * from a recording in the Aeron Archive. When the subscriber falls behind the live stream, it
 * switches to replaying from the archive. Once it catches up, it switches back to the live
 * stream.
 *
 * @see aeron_archive_persistent_subscription_t
 */
class PersistentSubscription
{
public:
    /**
     * Start consuming from the beginning of the recording.
     * @see aeron_archive_persistent_subscription_context_set_start_position
     */
    static constexpr std::int64_t FROM_START = AERON_PERSISTENT_SUBSCRIPTION_FROM_START;

    /**
     * Start consuming from the live stream, skipping any existing recorded data.
     * @see aeron_archive_persistent_subscription_context_set_start_position
     */
    static constexpr std::int64_t FROM_LIVE = AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE;

    /**
     * Configuration context for constructing a PersistentSubscription. Uses a fluent builder
     * pattern.
     *
     * The Context may safely go out of scope after PersistentSubscription::create() returns.
     */
    class Context
    {
        friend class PersistentSubscription;

    public:
        Context()
        {
            if (aeron_archive_persistent_subscription_context_init(&m_context_t) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }
        }

        ~Context()
        {
            aeron_archive_persistent_subscription_context_close(m_context_t);
        }

        Context(const Context &) = delete;
        Context &operator=(const Context &) = delete;
        Context(Context &&) = delete;
        Context &operator=(Context &&) = delete;

        /**
         * Set the Aeron Archive context for the archive connection used by this
         * persistent subscription. The archive context must be configured with
         * control channels, credentials, and other archive-specific settings
         * before being passed here.
         *
         * @param archiveContext the archive context.
         * @return this for fluent API.
         */
        inline Context &archiveContext(AeronArchive::Context_t &archiveContext)
        {
            if (aeron_archive_persistent_subscription_context_set_archive_context(
                m_context_t, archiveContext.m_aeron_archive_ctx_t) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Set the Aeron client to use. If not set, the PersistentSubscription will create its own.
         *
         * @param aeron the Aeron client.
         * @return this for fluent API.
         */
        inline Context &aeron(std::shared_ptr<Aeron> aeron)
        {
            if (aeron_archive_persistent_subscription_context_set_aeron(
                m_context_t, aeron->aeron()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            m_aeronW = std::move(aeron);
            return *this;
        }

        /**
         * Set the Aeron directory name for when the PersistentSubscription creates its own
         * Aeron client. Has no effect if an Aeron client is set via aeron().
         *
         * @param directoryName the Aeron directory.
         * @return this for fluent API.
         */
        inline Context &aeronDirectoryName(const std::string &directoryName)
        {
            if (aeron_archive_persistent_subscription_context_set_aeron_directory_name(
                m_context_t, directoryName.c_str()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Set the id of the recording to replay from.
         *
         * @param recordingId the recording id.
         * @return this for fluent API.
         */
        inline Context &recordingId(std::int64_t recordingId)
        {
            if (aeron_archive_persistent_subscription_context_set_recording_id(
                m_context_t, recordingId) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Get the recording ID.
         *
         * @return the recording ID.
         */
        inline std::int64_t recordingId() const
        {
            return aeron_archive_persistent_subscription_context_get_recording_id(m_context_t);
        }

        /**
         * Set the position to start consuming from. This can be a point in the recording,
         * the start of the recording (FROM_START) or directly from live (FROM_LIVE).
         *
         * @param startPosition the position to start consuming from.
         * @return this for fluent API.
         */
        inline Context &startPosition(std::int64_t startPosition)
        {
            if (aeron_archive_persistent_subscription_context_set_start_position(
                m_context_t, startPosition) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Get the start position.
         *
         * @return the start position.
         */
        inline std::int64_t startPosition() const
        {
            return aeron_archive_persistent_subscription_context_get_start_position(m_context_t);
        }

        /**
         * Set the channel to subscribe to for live data.
         *
         * @param channel the live channel URI.
         * @return this for fluent API.
         */
        inline Context &liveChannel(const std::string &channel)
        {
            if (aeron_archive_persistent_subscription_context_set_live_channel(
                m_context_t, channel.c_str()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Get the live channel.
         *
         * @return the live channel URI.
         */
        inline std::string liveChannel() const
        {
            return aeron_archive_persistent_subscription_context_get_live_channel(m_context_t);
        }

        /**
         * Set the stream ID for the live subscription.
         *
         * @param streamId the live stream ID.
         * @return this for fluent API.
         */
        inline Context &liveStreamId(std::int32_t streamId)
        {
            if (aeron_archive_persistent_subscription_context_set_live_stream_id(
                m_context_t, streamId) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Get the live stream ID.
         *
         * @return the live stream ID.
         */
        inline std::int32_t liveStreamId() const
        {
            return aeron_archive_persistent_subscription_context_get_live_stream_id(m_context_t);
        }

        /**
         * Set the channel used for receiving replayed data from the archive.
         *
         * @param channel the replay channel URI.
         * @return this for fluent API.
         */
        inline Context &replayChannel(const std::string &channel)
        {
            if (aeron_archive_persistent_subscription_context_set_replay_channel(
                m_context_t, channel.c_str()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Get the replay channel.
         *
         * @return the replay channel URI.
         */
        inline std::string replayChannel() const
        {
            return aeron_archive_persistent_subscription_context_get_replay_channel(m_context_t);
        }

        /**
         * Set the stream ID used for receiving replayed data from the archive.
         *
         * @param streamId the replay stream ID.
         * @return this for fluent API.
         */
        inline Context &replayStreamId(std::int32_t streamId)
        {
            if (aeron_archive_persistent_subscription_context_set_replay_stream_id(
                m_context_t, streamId) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            return *this;
        }

        /**
         * Get the replay stream ID.
         *
         * @return the replay stream ID.
         */
        inline std::int32_t replayStreamId() const
        {
            return aeron_archive_persistent_subscription_context_get_replay_stream_id(m_context_t);
        }

        /**
         * Set the counter for tracking the current state of the persistent subscription.
         * If not set, a counter will be auto-allocated during creation.
         *
         * @param counter the state counter.
         * @return this for fluent API.
         */
        inline Context &stateCounter(std::shared_ptr<Counter> counter)
        {
            if (aeron_archive_persistent_subscription_context_set_state_counter(
                m_context_t, counter->c_counter()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            m_stateCounter = std::move(counter);
            return *this;
        }

        /**
         * Set the counter for tracking the join difference of the persistent subscription.
         * If not set, a counter will be auto-allocated during creation.
         *
         * @param counter the join difference counter.
         * @return this for fluent API.
         */
        inline Context &joinDifferenceCounter(std::shared_ptr<Counter> counter)
        {
            if (aeron_archive_persistent_subscription_context_set_join_difference_counter(
                m_context_t, counter->c_counter()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            m_joinDifferenceCounter = std::move(counter);
            return *this;
        }

        /**
         * Set the counter for tracking the number of times the live stream has been left.
         * If not set, a counter will be auto-allocated during creation.
         *
         * @param counter the live left counter.
         * @return this for fluent API.
         */
        inline Context &liveLeftCounter(std::shared_ptr<Counter> counter)
        {
            if (aeron_archive_persistent_subscription_context_set_live_left_counter(
                m_context_t, counter->c_counter()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            m_liveLeftCounter = std::move(counter);
            return *this;
        }

        /**
         * Set the counter for tracking the number of times live has been joined.
         * If not set, a counter will be auto-allocated during creation.
         *
         * @param counter the live joined counter.
         * @return this for fluent API.
         */
        inline Context &liveJoinedCounter(std::shared_ptr<Counter> counter)
        {
            if (aeron_archive_persistent_subscription_context_set_live_joined_counter(
                m_context_t, counter->c_counter()) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            m_liveJoinedCounter = std::move(counter);
            return *this;
        }

        /**
         * Set a callback to be invoked when the persistent subscription transitions to
         * consuming from the live channel.
         *
         * @param callback the callback.
         * @return this for fluent API.
         */
        inline Context &onLiveJoined(std::function<void()> callback)
        {
            m_onLiveJoined = std::move(callback);
            return *this;
        }

        /**
         * Set a callback to be invoked when the persistent subscription stops consuming
         * from the live channel.
         *
         * @param callback the callback.
         * @return this for fluent API.
         */
        inline Context &onLiveLeft(std::function<void()> callback)
        {
            m_onLiveLeft = std::move(callback);
            return *this;
        }

        /**
         * Set a callback to be invoked when the subscription encounters an error.
         * Called for both terminal and non-terminal errors.
         *
         * @param callback the callback, receiving an error code and message.
         * @return this for fluent API.
         * @see PersistentSubscription::hasFailed
         */
        inline Context &onError(std::function<void(int, const std::string &)> callback)
        {
            m_onError = std::move(callback);
            return *this;
        }

    private:
        aeron_archive_persistent_subscription_context_t *m_context_t = nullptr;

        std::shared_ptr<Aeron> m_aeronW;

        std::function<void()> m_onLiveJoined;
        std::function<void()> m_onLiveLeft;
        std::function<void(int, const std::string &)> m_onError;

        std::shared_ptr<Counter> m_stateCounter;
        std::shared_ptr<Counter> m_joinDifferenceCounter;
        std::shared_ptr<Counter> m_liveLeftCounter;
        std::shared_ptr<Counter> m_liveJoinedCounter;
    };

    /**
     * Close the persistent subscription and dispose of all resources held by it.
     */
    ~PersistentSubscription()
    {
        aeron_archive_persistent_subscription_close(m_persistent_subscription_t);
    }

    PersistentSubscription(const PersistentSubscription &) = delete;
    PersistentSubscription &operator=(const PersistentSubscription &) = delete;
    PersistentSubscription(PersistentSubscription &&) = delete;
    PersistentSubscription &operator=(PersistentSubscription &&) = delete;

    /**
     * Create a PersistentSubscription from the given Context.
     * <p>
     * If no Aeron client is set, one will be created and owned by the PersistentSubscription.
     *
     * @param ctx the configuration context.
     * @return a shared_ptr to the new PersistentSubscription.
     * @throws ArchiveException if creation fails.
     */
    static std::shared_ptr<PersistentSubscription> create(Context &ctx)
    {
        std::shared_ptr<PersistentSubscription> subscription = std::shared_ptr<PersistentSubscription>(
            new PersistentSubscription(ctx.m_onLiveJoined, ctx.m_onLiveLeft, ctx.m_onError));

        aeron_archive_persistent_subscription_listener_t listener = {};
        listener.on_live_joined = onLiveJoinedFunc;
        listener.on_live_left = onLiveLeftFunc;
        listener.on_error = onErrorFunc;
        listener.clientd = subscription.get();

        if (aeron_archive_persistent_subscription_context_set_listener(
            ctx.m_context_t, &listener) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        aeron_archive_persistent_subscription_t *persistentSubscription = nullptr;

        if (aeron_archive_persistent_subscription_create(&persistentSubscription, ctx.m_context_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        subscription->m_persistent_subscription_t = persistentSubscription;
        ctx.m_context_t = nullptr;

        return subscription;
    }

    /**
     * Poll for available messages. Delivers assembled messages to the provided handler,
     * so the handler should not be a fragment assembler.
     *
     * The handler must be callable with signature:
     *   void(AtomicBuffer &buffer, util::index_t offset,
     *        util::index_t length, Header &header)
     *
     * @param fragmentHandler the handler for received message fragments.
     * @param fragmentLimit the maximum number of fragments to process per call.
     * @return positive if fragments have been read or the persistent subscription has done
     * other work, 0 if no fragments have been read and no work has been done.
     * @throws ArchiveException on error.
     */
    template<typename F>
    inline int poll(F &&fragmentHandler, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;

        int result = aeron_archive_persistent_subscription_poll(
            m_persistent_subscription_t,
            doPoll<handler_type>,
            const_cast<void *>(reinterpret_cast<const void *>(&fragmentHandler)),
            static_cast<size_t>(fragmentLimit));

        if (result < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return result;
    }

    /**
     * Poll in a controlled manner for available messages. Delivers assembled messages to the
     * provided handler, so the handler should not be a fragment assembler.
     *
     * The handler must be callable with signature:
     *   ControlledPollAction(AtomicBuffer &buffer, util::index_t offset,
     *                        util::index_t length, Header &header)
     *
     * @param fragmentHandler the handler for received message fragments.
     * @param fragmentLimit the maximum number of fragments to process per call.
     * @return positive if fragments have been read or the persistent subscription has done
     * other work, 0 if no fragments have been read and no work has been done.
     * @throws ArchiveException on error.
     */
    template<typename F>
    inline int controlledPoll(F &&fragmentHandler, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;

        int result = aeron_archive_persistent_subscription_controlled_poll(
            m_persistent_subscription_t,
            doControlledPoll<handler_type>,
            const_cast<void *>(reinterpret_cast<const void *>(&fragmentHandler)),
            static_cast<size_t>(fragmentLimit));

        if (result < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return result;
    }

    /**
     * Indicates if the persistent subscription is live, i.e. consuming messages from the live
     * subscription.
     *
     * @return true if live, false otherwise.
     */
    inline bool isLive() const
    {
        return aeron_archive_persistent_subscription_is_live(m_persistent_subscription_t);
    }

    /**
     * Indicates if the persistent subscription is replaying from a recording.
     *
     * @return true if replaying from a recording, false otherwise.
     */
    inline bool isReplaying() const
    {
        return aeron_archive_persistent_subscription_is_replaying(m_persistent_subscription_t);
    }

    /**
     * Indicates if the persistent subscription has failed.
     * <p>
     * The listener will be notified of any terminal errors that can cause the persistent
     * subscription to fail.
     *
     * @return true if failed, false otherwise.
     * @see Context::onError
     */
    inline bool hasFailed() const
    {
        return aeron_archive_persistent_subscription_has_failed(m_persistent_subscription_t);
    }

private:
    aeron_archive_persistent_subscription_t *m_persistent_subscription_t = nullptr;

    std::function<void()> m_onLiveJoined;
    std::function<void()> m_onLiveLeft;
    std::function<void(int, const std::string &)> m_onError;

    PersistentSubscription(
        std::function<void()> onLiveJoined,
        std::function<void()> onLiveLeft,
        std::function<void(int, const std::string &)> onError) :
        m_onLiveJoined(std::move(onLiveJoined)),
        m_onLiveLeft(std::move(onLiveLeft)),
        m_onError(std::move(onError))
    {
    }

    static void onLiveJoinedFunc(void *clientd) noexcept
    {
        try
        {
            PersistentSubscription *self = reinterpret_cast<PersistentSubscription *>(clientd);
            if (self->m_onLiveJoined)
            {
                self->m_onLiveJoined();
            }
        }
        catch (const std::exception &ex)
        {
            AERON_APPEND_ERR("%s", ex.what());
        }
    }

    static void onLiveLeftFunc(void *clientd) noexcept
    {
        try
        {
            PersistentSubscription *self = reinterpret_cast<PersistentSubscription *>(clientd);
            if (self->m_onLiveLeft)
            {
                self->m_onLiveLeft();
            }
        }
        catch (const std::exception &ex)
        {
            AERON_APPEND_ERR("%s", ex.what());
        }
    }

    static void onErrorFunc(void *clientd, int errcode, const char *message) noexcept
    {
        try
        {
            PersistentSubscription *self = reinterpret_cast<PersistentSubscription *>(clientd);
            if (self->m_onError)
            {
                self->m_onError(errcode, std::string(message));
            }
        }
        catch (const std::exception &ex)
        {
            AERON_APPEND_ERR("%s", ex.what());
        }
    }
};

}}}

#endif //AERON_ARCHIVE_WRAPPER_PERSISTENT_SUBSCRIPTION_H
