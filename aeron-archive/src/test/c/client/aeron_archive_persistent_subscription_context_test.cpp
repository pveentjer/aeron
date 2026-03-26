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

#include "ArchiveClientTestUtils.h"
#include "gtest/gtest.h"

extern "C"
{
#include "client/aeron_archive_persistent_subscription_internal.h"
}

class AeronArchivePersistentSubscriptionContextTest : public testing::Test
{
};

TEST_F(AeronArchivePersistentSubscriptionContextTest, testValidContextWithExternalAeron)
{
    aeron_archive_context_t *archive_context;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_context)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_archive_context(context, archive_context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_recording_id(context, 0)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_stream_id(context, 2000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_aeron(context, reinterpret_cast<aeron_t*>(8))) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_conclude(context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_close(context)) << aeron_errmsg();

    EXPECT_EQ(0, aeron_archive_context_close(archive_context)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, testValidContextWithOwnedAeron)
{
    DriverResource driver;

    aeron_archive_context_t *archive_context;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_context)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_archive_context(context, archive_context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_recording_id(context, 0)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_stream_id(context, 2000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_aeron_directory_name(context, driver.aeronDir().c_str())) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_conclude(context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_close(context)) << aeron_errmsg();

    EXPECT_EQ(0, aeron_archive_context_close(archive_context)) << aeron_errmsg();
}

struct ReplayAndControlChannels
{
    int expectedConcludeResult;
    const char *replayChannel;
    const char *controlRequestChannel;
    const char *controlResponseChannel;
};

class ReplayAndControlChannelsTest : public AeronArchivePersistentSubscriptionContextTest,
                                     public testing::WithParamInterface<ReplayAndControlChannels>
{
};

TEST_P(ReplayAndControlChannelsTest, replayAndControlChannelMediaTypesMustMatchWhenUsingResponseChannels)
{
    const auto& param = GetParam();

    aeron_archive_context_t *archive_context;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_context)) << aeron_errmsg();
    aeron_archive_context_set_control_request_channel(archive_context, param.controlRequestChannel);
    aeron_archive_context_set_control_response_channel(archive_context, param.controlResponseChannel);

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_archive_context(context, archive_context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_recording_id(context, 0)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_channel(context, param.replayChannel)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_stream_id(context, 2000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_aeron(context, reinterpret_cast<aeron_t*>(8))) << aeron_errmsg();
    EXPECT_EQ(param.expectedConcludeResult, aeron_archive_persistent_subscription_context_conclude(context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_close(context)) << aeron_errmsg();

    EXPECT_EQ(0, aeron_archive_context_close(archive_context)) << aeron_errmsg();
}

INSTANTIATE_TEST_SUITE_P(, ReplayAndControlChannelsTest, testing::Values(
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", nullptr, nullptr},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc?control-mode=response"},
    ReplayAndControlChannels{0, "aeron:udp?control=localhost:10001|control-mode=response", nullptr, nullptr},
    ReplayAndControlChannels{0, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{0, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{-1, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{-1, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:ipc", "aeron:ipc?control-mode=response"},
    ReplayAndControlChannels{0, "aeron:ipc", nullptr, nullptr},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:ipc", "aeron:ipc?control-mode=response"},
    ReplayAndControlChannels{0, "aeron:ipc?control-mode=response", nullptr, nullptr},
    ReplayAndControlChannels{-1, "aeron:ipc?control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{-1, "aeron:ipc?control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{0, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{0, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc?control-mode=response"}
));
