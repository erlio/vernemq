-module(vmq_publish_SUITE).

-include_lib("vmq_commons/include/vmq_types.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-include("../src/vmq_metrics.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    cover:start(),
    vmq_test_utils:setup(),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    [S, {ct_hooks, vmq_cth} |Config].

end_per_suite(_Config) ->
    vmq_server_cmd:listener_stop(1888, "127.0.0.1", false),
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv3, Config) ->
    Config;
init_per_group(mqttv4, Config) ->
    [{protover, 4}|Config];
init_per_group(mqttv5, Config) ->
    [{protover, 5}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 2),
    vmq_server_cmd:set_config(max_client_id_size, 100),
    vmq_server_cmd:set_config(topic_alias_max_client, 0),
    vmq_server_cmd:set_config(topic_alias_max_broker, 0),
    case lists:member(Case, [shared_subscription_offline,
                             shared_subscription_online_first]) of
        true ->
            start_client_offline_events(Config);
        _ ->
            Config
    end.

end_per_testcase(Case, Config) ->
    case lists:member(Case, [shared_subscription_offline,
                             shared_subscription_online_first]) of
        true ->
            stop_client_offline_events(Config);
        _ ->
            Config
    end.

all() ->
    [
     {group, mqttv3},
     {group, mqttv4},
     {group, mqttv5}
    ].

groups() ->
    V4V5Tests =
        [publish_qos1_test,
         publish_qos2_test,
         publish_to_dollar_topic,
         publish_b2c_qos2_duplicate_test,
         publish_c2b_qos2_duplicate_test,
         publish_b2c_disconnect_qos1_test,
         publish_b2c_disconnect_qos2_test,
         publish_c2b_disconnect_qos2_test,
         publish_b2c_ensure_valid_msg_ids_test,
         pattern_matching_test,
         drop_dollar_sys_topic_publish,
         message_size_exceeded_close,
         shared_subscription_offline,
         shared_subscription_online_first,
         direct_plugin_exports_test
        ],
    [
     {mqttv3, [shuffle], [
                   not_allowed_publish_close_qos0_mqtt_3_1,
                   not_allowed_publish_close_qos1_mqtt_3_1,
                   not_allowed_publish_close_qos2_mqtt_3_1,
                   message_size_exceeded_close]},
     {mqttv4, [shuffle], [
                   not_allowed_publish_close_qos0_mqtt_3_1_1,
                   not_allowed_publish_close_qos1_mqtt_3_1_1,
                   not_allowed_publish_close_qos2_mqtt_3_1_1,
                   message_size_exceeded_close,
                   publish_c2b_retry_qos2_test,
                   publish_b2c_retry_qos1_test,
                   publish_b2c_retry_qos2_test
                   | V4V5Tests] },
     {mqttv5, [shuffle], [
                   not_allowed_publish_qos0_mqtt_5,
                   not_allowed_publish_qos1_mqtt_5,
                   not_allowed_publish_qos2_mqtt_5,
                   message_expiry_interval,
                   publish_c2b_topic_alias,
                   publish_b2c_topic_alias,
                   forward_properties,
                   max_packet_size
                   | V4V5Tests] }
    ].

-define(CLIENT_OFFLINE_EVENT_SRV, vmq_client_offline_event_server).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
publish_qos1_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("pub-qos1-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish("pub/qos1/test", 1, <<"message">>,
                                         [{mid, 19}], Config),
    Puback = mqtt5_v4compat:gen_puback(19, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Config),
    disable_on_publish(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_to_dollar_topic(Config) ->
    Connect = mqtt5_v4compat:gen_connect("publish-dollar-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Topic = "$test/drop",
    Publish = mqtt5_v4compat:gen_publish(Topic, 1, <<"message">>, [{mid, 19}], Config),
    Puback = mqtt5_v4compat:gen_puback(19, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Config),
    disable_on_publish(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_qos2_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("pub-qos2-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish("pub/qos2/test", 2, <<"message">>,
                                 [{mid, 312}], Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(312, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(312, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(312, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubcomp", Pubcomp, Config),
    disable_on_publish(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_b2c_qos2_duplicate_test(Cfg) ->
    %% Ensure retried pubrecs from the client are handled correctly.
    %%
    %% MQTT 3.1.1 [MQTT-4.3.3-1]: sender receiver MUST send a PUBREL
    %% packet when it receives a PUBREC packet from the receiver. This
    %% PUBREL packet MUST contain the same Packet Identifier as the
    %% original PUBLISH packet.
    %%
    %% MQTT 5.0 [MQTT-4.3.3-4]: sender MUST send a PUBREL packet when
    %% it receives a PUBREC packet from the receiver with a Reason
    %% Code value less than 0x80. This PUBREL packet MUST contain the
    %% same Packet Identifier as the original PUBLISH packet.
    PubId = vmq_cth:ustr(Cfg) ++ "-pub",
    Topic = vmq_cth:utopic(Cfg),
    ConnectPub = mqtt5_v4compat:gen_connect(PubId, [], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 2, <<"message">>,
                                         [{mid, 312}], Cfg),
    Pubrec = mqtt5_v4compat:gen_pubrec(312, Cfg),
    Pubrel = mqtt5_v4compat:gen_pubrel(312, Cfg),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(312, Cfg),
    {ok, PubSocket} = mqtt5_v4compat:do_client_connect(ConnectPub, Connack, [], Cfg),

    SubId = vmq_cth:ustr(Cfg) ++ "-sub",
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, Topic, 2, Cfg),
    Suback = mqtt5_v4compat:gen_suback(3265, 2, Cfg),
    ConnectSub = mqtt5_v4compat:gen_connect(SubId, [], Cfg),
    {ok, SubSocket} = mqtt5_v4compat:do_client_connect(ConnectSub, Connack, [], Cfg),


    enable_on_publish(),
    enable_on_subscribe(),

    %% subscribe to the topic
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(SubSocket, "suback", Suback, Cfg),

    %% publish a Qos2 message
    ok = gen_tcp:send(PubSocket, Publish),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubrec", Pubrec, Cfg),
    ok = gen_tcp:send(PubSocket, Pubrel),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubcomp", Pubcomp, Cfg),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(PubSocket),
    ok = gen_tcp:close(PubSocket),

    %% verify that we only recieve the first published message and not
    %% the duplicate.
    ExpectF =
        fun(Name, What) ->
                mqtt5_v4compat:expect_packet(SubSocket, Name, What, Cfg)
        end,

    RecvPublish = mqtt5_v4compat:gen_publish(Topic, 2, <<"message">>, [{mid, 1}], Cfg),
    ok = ExpectF("publish", RecvPublish),
    ok = gen_tcp:send(SubSocket, mqtt5_v4compat:gen_pubrec(1, Cfg)),
    ok = ExpectF("pubrel", mqtt5_v4compat:gen_pubrel(1, Cfg)),
    %% now resend the pubrec - the broker must respond with a pubrel
    ok = gen_tcp:send(SubSocket, mqtt5_v4compat:gen_pubrec(1, Cfg)),
    ok = ExpectF("pubrel", mqtt5_v4compat:gen_pubrel(1, Cfg)),

    ok = gen_tcp:send(SubSocket, mqtt5_v4compat:gen_pubcomp(1, Cfg)),
    ok = expect_alive(SubSocket),
    ok = gen_tcp:close(SubSocket).

publish_c2b_qos2_duplicate_test(Cfg) ->
    %% assure that we don't forward duplicates if the client
    %% republishes a duplicate during a QoS2 flow.
    PubId = vmq_cth:ustr(Cfg) ++ "-pub",
    Topic = vmq_cth:utopic(Cfg),
    ConnectPub = mqtt5_v4compat:gen_connect(PubId, [], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 2, <<"message">>,
                                 [{mid, 312}], Cfg),
    PublishDup = mqtt5_v4compat:gen_publish(Topic, 2, <<"message">>,
                                 [{mid, 312}], Cfg),
    Pubrec = mqtt5_v4compat:gen_pubrec(312, Cfg),
    Pubrel = mqtt5_v4compat:gen_pubrel(312, Cfg),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(312, Cfg),
    {ok, PubSocket} = mqtt5_v4compat:do_client_connect(ConnectPub, Connack, [], Cfg),

    SubId = vmq_cth:ustr(Cfg) ++ "-sub",
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(3265, 0, Cfg),
    RecvPublish = mqtt5_v4compat:gen_publish(Topic, 0, <<"message">>, [], Cfg),
    ConnectSub = mqtt5_v4compat:gen_connect(SubId, [], Cfg),
    {ok, SubSocket} = mqtt5_v4compat:do_client_connect(ConnectSub, Connack, [], Cfg),


    enable_on_publish(),
    enable_on_subscribe(),

    %% subscribe to the topic
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(SubSocket, "suback", Suback, Cfg),

    %% start qos2 flow with a duplicate publish
    ok = gen_tcp:send(PubSocket, Publish),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubrec", Pubrec, Cfg),
    ok = gen_tcp:send(PubSocket, PublishDup),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubrec", Pubrec, Cfg),
    ok = gen_tcp:send(PubSocket, Pubrel),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubcomp", Pubcomp, Cfg),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(PubSocket),
    ok = gen_tcp:close(PubSocket),

    %% verify that we only recieve the first published message and not
    %% the duplicate.
    ok = mqtt5_v4compat:expect_packet(SubSocket, "publish", RecvPublish, Cfg),
    %%ok = mqtt5_v4compat:expect_packet(SubSocket, "publish", RecvPublish, Cfg),
    {error, timeout} = gen_tcp:recv(SubSocket, 0, 1000),
    ok = expect_alive(SubSocket),
    ok = gen_tcp:close(SubSocket).

publish_b2c_disconnect_qos1_test(Config) ->
    ClientId = vmq_cth:ustr(Config) ++ "pub-qos1-disco-test",
    Connect = mqtt5_v4compat:gen_connect(ClientId,
                                         [{keepalive, 60}, {clean_session, false}], Config),
    Connack1 = mqtt5_v4compat:gen_connack(success, Config),
    Connack2 = mqtt5_v4compat:gen_connack(true, success, Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, "qos1/disconnect/test", 1, Config),
    Suback = mqtt5_v4compat:gen_suback(3265, 1, Config),
    Publish = mqtt5_v4compat:gen_publish("qos1/disconnect/test", 1,
                                         <<"disconnect-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos1/disconnect/test", 1,
                                            <<"disconnect-message">>,
                                            [{mid, 1}, {dup, true}], Config),
    Puback = mqtt5_v4compat:gen_puback(1, Config),
    Publish2 = mqtt5_v4compat:gen_publish("qos1/outgoing", 1,
                                  <<"outgoing-message">>, [{mid, 3266}], Config),
    Puback2 = packet:gen_puback(3266),

    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack1, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pub_qos1("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = packet:expect_packet(Socket, "puback", Puback2),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = mqtt5_v4compat:expect_packet(Socket1, "dup publish", PublishDup, Config),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket1),
    ok = gen_tcp:close(Socket1).

publish_b2c_disconnect_qos2_test(Config) ->
    ClientId = vmq_cth:ustr(Config) ++ "pub-b2c-qos2-disco-test",
    Connect = mqtt5_v4compat:gen_connect(ClientId,
                                 [{keepalive, 60}, {clean_session, false}], Config),
    Connack1 = mqtt5_v4compat:gen_connack(success, Config),
    Connack2 = mqtt5_v4compat:gen_connack(true, success, Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, "qos2/disconnect/test", 2, Config),
    Suback = mqtt5_v4compat:gen_suback(3265, 2, Config),
    Publish = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                 <<"disconnect-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                    <<"disconnect-message">>,
                                    [{mid, 1}, {dup, true}], Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(1, Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(1, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(1, Config),
    Publish2 = mqtt5_v4compat:gen_publish("qos1/outgoing", 1,
                                  <<"outgoing-message">>, [{mid, 3266}], Config),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack1, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pub_qos2("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = mqtt5_v4compat:expect_packet(Socket1, "dup publish", PublishDup, Config),
    ok = gen_tcp:send(Socket1, Pubrec),
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubrel", Pubrel, Config),
    ok = gen_tcp:close(Socket1),
    %% Expect Pubrel
    {ok, Socket2} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = mqtt5_v4compat:expect_packet(Socket2, "pubrel", Pubrel, Config),
    ok = gen_tcp:send(Socket2, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket2),
    ok = gen_tcp:close(Socket2).

publish_b2c_retry_qos1_test(Config) ->
    Connect = packet:gen_connect("pub-qos1-timeout-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos1/timeout/test", 1),
    Suback = packet:gen_suback(3265, 1),
    Publish = packet:gen_publish("qos1/timeout/test", 1,
                                 <<"timeout-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos1/timeout/test", 1,
                                    <<"timeout-message">>,
                                    [{mid, 1}, {dup, true}]),
    Puback = packet:gen_puback(1),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos1("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish),
    %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = packet:expect_packet(Socket, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_b2c_retry_qos2_test(Config) ->
    Connect = packet:gen_connect("pub-b2c-qos2-timeout-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos2/timeout/test", 2),
    Suback = packet:gen_suback(3265, 2),
    Publish = packet:gen_publish("qos2/timeout/test", 2,
                                 <<"timeout-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos2/timeout/test", 2,
                                    <<"timeout-message">>,
                                    [{mid, 1}, {dup, true}]),
    Pubrec = packet:gen_pubrec(1),
    Pubrel = packet:gen_pubrel(1),
    Pubcomp = packet:gen_pubcomp(1),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos2("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish),
    %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = packet:expect_packet(Socket, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket, Pubrec),
    ok = packet:expect_packet(Socket, "pubrel", Pubrel),
    %% The broker should repeat the PUBREL NO dup set according to MQTT-3.6.1-1
    ok = packet:expect_packet(Socket, "pubrel", Pubrel),
    ok = gen_tcp:send(Socket, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_c2b_disconnect_qos2_test(Config) ->
    ClientId = vmq_cth:ustr(Config) ++ "pub-c2b-qos2-disco-test",
    Connect = mqtt5_v4compat:gen_connect(ClientId,
                                         [{keepalive, 60}, {clean_session, false}], Config),
    Connack1 = mqtt5_v4compat:gen_connack(success, Config),
    Connack2 = mqtt5_v4compat:gen_connack(true, success, Config),
    Publish = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                         <<"disconnect-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                            <<"disconnect-message">>,
                                            [{mid, 1}, {dup, true}], Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(1, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(1, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(1, Config),
    enable_on_publish(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack1, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    %% We're now going to disconnect and pretend we didn't receive the pubrec
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket),
    {ok, Socket1} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = gen_tcp:send(Socket1, PublishDup),
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubrec", Pubrec, Config),
    ok = gen_tcp:send(Socket1, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubcomp", Pubcomp, Config),
    %% Again, pretend we didn't receive this pubcomp
    ok = expect_alive(Socket1),
    ok = gen_tcp:close(Socket1),
    {ok, Socket2} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = gen_tcp:send(Socket2, Pubrel),
    Pubcomp2 =
    case mqtt5_v4compat:protover(Config) of
        5 -> packetv5:gen_pubcomp(1, ?M5_PACKET_ID_NOT_FOUND, #{});
        _ -> Pubcomp
    end,
    ok = mqtt5_v4compat:expect_packet(Socket2, "pubcomp", Pubcomp2, Config),
    disable_on_publish(),
    ok = expect_alive(Socket2),
    ok = gen_tcp:close(Socket2).

publish_c2b_retry_qos2_test(_Config) ->
    Connect = packet:gen_connect("pub-c2b-qos2-timeout-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/qos2/test", 2,
                                 <<"timeout-message">>, [{mid, 1926}]),
    Pubrec = packet:gen_pubrec(1926),
    Pubrel = packet:gen_pubrel(1926),
    Pubcomp = packet:gen_pubcomp(1926),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    disable_on_publish(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_b2c_ensure_valid_msg_ids_test(Config) ->
    %% ensure that a stored pub_rel with msg id X (here 1) isn't
    %% overwritten by a new published message.
    enable_on_publish(),
    enable_on_subscribe(),
    ClientId = vmq_cth:ustr(Config) ++ "-persisted",
    Topic = vmq_cth:utopic(Config) ++ "/client",
    Connect = mqtt5_v4compat:gen_connect(ClientId,
                                         [{keepalive, 60},
                                          {clean_session, false}],
                                         Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    %% subscribe to the offline topic
    Subscribe = mqtt5_v4compat:gen_subscribe(1, Topic, 2, Config),
    Suback = mqtt5_v4compat:gen_suback(1, 2, Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),

    %% connect publisher
    PubClientId = vmq_cth:ustr(Config) ++ "-publisher",
    PubConnect = mqtt5_v4compat:gen_connect(PubClientId,
                                            [{keepalive, 60},
                                             {clean_session, true}],
                                            Config),
    {ok, PubSocket} = mqtt5_v4compat:do_client_connect(PubConnect, Connack, [], Config),
    Publish1 = mqtt5_v4compat:gen_publish(Topic, 2, <<"msg1">>, [{mid, 1}], Config),
    Pubrec1 = mqtt5_v4compat:gen_pubrec(1, Config),
    Pubrel1 = mqtt5_v4compat:gen_pubrel(1, Config),
    Pubcomp1 = mqtt5_v4compat:gen_pubcomp(1, Config),

    %% publish qos2 message
    ok = gen_tcp:send(PubSocket, Publish1),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubrec", Pubrec1, Config),
    ok = gen_tcp:send(PubSocket, Pubrel1),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubcomp", Pubcomp1, Config),

    %% receive message, but disconnect after sending pubrec
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish1, Config),
    ok = gen_tcp:send(Socket, Pubrec1),
    Disconnect = mqtt5_v4compat:gen_disconnect(Config),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% Now reconnect
    ConnackSP = mqtt5_v4compat:gen_connack(true, success, Config),
    {ok, Socket1} = mqtt5_v4compat:do_client_connect(Connect, ConnackSP, [], Config),

    %% Now we should receive the retried pubrel from the broker:
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubrel", Pubrel1, Config),

    %% Then publish another message which *should* not collide with the pubrel from before.
    Publish2 = mqtt5_v4compat:gen_publish(Topic, 2, <<"msg2">>, [{mid, 2}], Config),
    Pubrec2 = mqtt5_v4compat:gen_pubrec(2, Config),
    Pubrel2 = mqtt5_v4compat:gen_pubrel(2, Config),
    Pubcomp2 = mqtt5_v4compat:gen_pubcomp(2, Config),

    ok = gen_tcp:send(PubSocket, Publish2),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubrec", Pubrec2, Config),
    ok = gen_tcp:send(PubSocket, Pubrel2),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "pubcomp", Pubcomp2, Config),

    %% and we should now be able to send the pubcomp for the pubrel from before.
    ok = gen_tcp:send(Socket1, Pubcomp1),

    %% and we can now receive the second message
    ok = mqtt5_v4compat:expect_packet(Socket1, "publish", Publish2, Config),
    ok = gen_tcp:send(Socket1, Pubrec2),
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubrel", Pubrel2, Config),
    ok = gen_tcp:send(Socket1, Pubcomp2),
    Disconnect = mqtt5_v4compat:gen_disconnect(Config),

    %% connect subscriber,
    disable_on_subscribe(),
    disable_on_publish(),

    ok = expect_alive(Socket1),
    ok = gen_tcp:send(Socket1, Disconnect),
    ok = gen_tcp:close(Socket1).


pattern_matching_test(Config) ->
    ok = pattern_test("#", "test/topic", Config),
    ok = pattern_test("#", "/test/topic", Config),
    ok = pattern_test("foo/#", "foo/bar/baz", Config),
    ok = pattern_test("foo/+/baz", "foo/bar/baz", Config),
    ok = pattern_test("foo/+/baz/#", "foo/bar/baz", Config),
    ok = pattern_test("foo/+/baz/#", "foo/bar/baz/bar", Config),
    ok = pattern_test("foo/foo/baz/#", "foo/foo/baz/bar", Config),
    ok = pattern_test("foo/#", "foo", Config),
    ok = pattern_test("/#", "/foo", Config),
    ok = pattern_test("test/topic/", "test/topic/", Config),
    ok = pattern_test("test/topic/+", "test/topic/", Config),
    ok = pattern_test("+/+/+/+/+/+/+/+/+/test",
                      "one/two/three/four/five/six/seven/eight/nine/test", Config),
    ok = pattern_test("#", "test////a//topic", Config),
    ok = pattern_test("#", "/test////a//topic", Config),
    ok = pattern_test("foo/#", "foo//bar///baz", Config),
    ok = pattern_test("foo/+/baz", "foo//baz", Config),
    ok = pattern_test("foo/+/baz//", "foo//baz//", Config),
    ok = pattern_test("foo/+/baz/#", "foo//baz", Config),
    ok = pattern_test("foo/+/baz/#", "foo//baz/bar", Config),
    ok = pattern_test("foo//baz/#", "foo//baz/bar", Config),
    ok = pattern_test("foo/foo/baz/#", "foo/foo/baz/bar", Config),
    ok = pattern_test("/#", "////foo///bar", Config).

pattern_test(SubTopic, PubTopic, Config) ->
    Connect = mqtt5_v4compat:gen_connect("pattern-sub-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish(PubTopic, 0, <<"message">>, [], Config),
    PublishRetained = mqtt5_v4compat:gen_publish(PubTopic, 0, <<"message">>,
                                         [{retain, true}], Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(312, SubTopic, 0, Config),
    Suback = mqtt5_v4compat:gen_suback(312, 0, Config),
    Unsubscribe = mqtt5_v4compat:gen_unsubscribe(234, SubTopic, Config),
    Unsuback = mqtt5_v4compat:gen_unsuback(234, Config),
    enable_on_publish(),
    enable_on_subscribe(),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pattern_matching(PubTopic, Config),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "unsuback", Unsuback, Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish retained", PublishRetained, Config),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

not_allowed_publish_close_qos0_mqtt_3_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 0, <<"message">>, []),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, timeout} = gen_tcp:recv(Socket, 0, 1000),
    gen_tcp:close(Socket).

not_allowed_publish_close_qos1_mqtt_3_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 1, <<"message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    {error, timeout} = gen_tcp:recv(Socket, 0, 1000),
    gen_tcp:close(Socket).

not_allowed_publish_close_qos2_mqtt_3_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 2, <<"message">>, [{mid, 1}]),
    Pubrec = packet:gen_pubrec(1),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    %% we receive proper pubrec
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    gen_tcp:close(Socket).

not_allowed_publish_close_qos0_mqtt_3_1_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {proto_ver, 4}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 0, <<"message">>, []),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000).

not_allowed_publish_close_qos1_mqtt_3_1_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {proto_ver, 4}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 1, <<"message">>, [{mid, 1}]),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000).

not_allowed_publish_close_qos2_mqtt_3_1_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {proto_ver, 4}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 2, <<"message">>, [{mid, 1}]),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000).

drop_dollar_sys_topic_publish(Config) ->
    Connect = mqtt5_v4compat:gen_connect("drop-dollar-sys-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Topic = "$SYS/drop",
    Publish = mqtt5_v4compat:gen_publish(Topic, 1, <<"message">>, [{mid, 1}], Config),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    gen_tcp:send(Socket, Publish),
    % receive a timeout instead of a PUBACk
    {error, timeout} = gen_tcp:recv(Socket, 0, 1000).

not_allowed_publish_qos0_mqtt_5(_) ->
    Connect = packetv5:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {clean_start, true}]),
    Connack = packetv5:gen_connack(),
    Topic = "test/topic/not_allowed",
    Publish = packetv5:gen_publish(Topic, 0, <<"message">>, [{mid, 1}]),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    % check connection isn't closed
    ok = gen_tcp:send(Socket, <<>>),
    ok = gen_tcp:close(Socket).

not_allowed_publish_qos1_mqtt_5(_) ->
    Connect = packetv5:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {clean_start, true}]),
    Connack = packetv5:gen_connack(),
    Topic = "test/topic/not_allowed",
    Publish = packetv5:gen_publish(Topic, 1, <<"message">>, [{mid, 1}]),
    Puback = packetv5:gen_puback(1, ?M5_NOT_AUTHORIZED, #{}),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    ok = packetv5:expect_frame(Socket, Puback),
    ok = gen_tcp:close(Socket).

not_allowed_publish_qos2_mqtt_5(_) ->
    Connect = packetv5:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                        {clean_start, true}]),
    Connack = packetv5:gen_connack(),
    Topic = "test/topic/not_allowed",
    Publish = packetv5:gen_publish(Topic, 2, <<"message">>, [{mid, 1}]),
    Pubrec = packetv5:gen_pubrec(1, ?M5_NOT_AUTHORIZED, #{}),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    ok = packetv5:expect_frame(Socket, Pubrec),
    ok = gen_tcp:close(Socket).

message_size_exceeded_close(_) ->
    OldLimit = vmq_config:get_env(max_message_size),
    vmq_config:set_env(max_message_size, 1024, false),
    vmq_metrics:reset_counters(),
    Connect = packet:gen_connect("pub-excessive-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/excessive/test", 0, vmq_test_utils:rand_bytes(1024),
                                 [{mid, 19}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000),
    true = lists:any(
             fun({#metric_def{
                     type = counter,
                     name = mqtt_invalid_msg_size_error}, 1}) -> true;
                (_) -> false
             end,vmq_metrics:metrics(#{aggregate => true})),
    vmq_config:set_env(max_message_size, OldLimit, false),
    disable_on_publish().

shared_subscription_offline(Cfg) ->
    enable_on_publish(),
    enable_on_subscribe(),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Prefix = vmq_cth:ustr(Cfg),
    PubConnect = mqtt5_v4compat:gen_connect(Prefix ++ "single-offline-pub", [{keepalive, 60}], Cfg),
    SubOfflineClientId = Prefix ++ "single-offline-sha-sub",
    SubConnectOffline = mqtt5_v4compat:gen_connect(SubOfflineClientId,
                                                   [{keepalive, 60},
                                                    {clean_session, false}], Cfg),
    Subscription = "$share/" ++ Prefix ++ "/shared_sub_topic",
    {ok, PubSocket} = mqtt5_v4compat:do_client_connect(PubConnect, Connack, [], Cfg),
    {ok, SubSocketOffline} = mqtt5_v4compat:do_client_connect(SubConnectOffline, Connack, [], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(1, Subscription, 1, Cfg),
    Suback = mqtt5_v4compat:gen_suback(1, 1, Cfg),
    ok = gen_tcp:send(SubSocketOffline, Subscribe),
    ok = mqtt5_v4compat:expect_packet(SubSocketOffline, "suback", Suback, Cfg),
    Disconnect = mqtt5_v4compat:gen_disconnect(Cfg),
    ok = gen_tcp:send(SubSocketOffline, Disconnect),

    %% wait for the client to be offline before publishing
    wait_for_offline_event(SubOfflineClientId, 500),

    PubFun
        = fun(Socket, Mid) ->
                  Publish = mqtt5_v4compat:gen_publish("shared_sub_topic", 1,
                                                       <<Mid:8, (vmq_test_utils:rand_bytes(10))/binary>>,
                                                       [{mid, Mid}], Cfg),
                  Puback = mqtt5_v4compat:gen_puback(Mid, Cfg),
                  ok = gen_tcp:send(Socket, Publish),
                  ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Cfg),
                  {Mid, Publish}
          end,
    Published = [PubFun(PubSocket, Mid) || Mid <- lists:seq(1,10)],
    ConnackSP = mqtt5_v4compat:gen_connack(true, success, Cfg),
    {ok, SubSocketOffline2} = mqtt5_v4compat:do_client_connect(SubConnectOffline, ConnackSP, [], Cfg),
    [begin
         ok = mqtt5_v4compat:expect_packet(SubSocketOffline2, "publish", Expect, Cfg),
         Puback = mqtt5_v4compat:gen_puback(Mid, Cfg),
         ok = gen_tcp:send(SubSocketOffline2, Puback)
     end || {Mid, Expect} <- Published ],
    disable_on_publish(),
    disable_on_subscribe().

shared_subscription_online_first(Cfg) ->
    enable_on_publish(),
    enable_on_subscribe(),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Prefix = vmq_cth:ustr(Cfg),
    PubConnect = mqtt5_v4compat:gen_connect(Prefix ++ "shared-sub-pub", [{keepalive, 60}], Cfg),
    SubConnectOnline = mqtt5_v4compat:gen_connect(Prefix ++ "shared-sub-sub-online",
                                                  [{keepalive, 60},
                                                   {clean_session, false}], Cfg),
    SubOfflineClientId = Prefix ++ "shared-sub-sub-offline",
    SubConnectOffline = mqtt5_v4compat:gen_connect(SubOfflineClientId,
                                                   [{keepalive, 60},
                                                    {clean_session, false}],
                                                   Cfg),
    Subscription = "$share/" ++ Prefix ++ "/shared_sub_topic",
    {ok, PubSocket} = mqtt5_v4compat:do_client_connect(PubConnect, Connack, [], Cfg),
    {ok, SubSocketOnline} = mqtt5_v4compat:do_client_connect(SubConnectOnline, Connack, [], Cfg),
    {ok, SubSocketOffline} = mqtt5_v4compat:do_client_connect(SubConnectOffline, Connack, [], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(1, Subscription, 1, Cfg),
    Suback = mqtt5_v4compat:gen_suback(1, 1, Cfg),
    ok = gen_tcp:send(SubSocketOffline, Subscribe),
    ok = mqtt5_v4compat:expect_packet(SubSocketOffline, "suback", Suback, Cfg),
    ok = gen_tcp:send(SubSocketOnline, Subscribe),
    ok = mqtt5_v4compat:expect_packet(SubSocketOnline, "suback", Suback, Cfg),

    Disconnect = mqtt5_v4compat:gen_disconnect(Cfg),
    ok = gen_tcp:send(SubSocketOffline, Disconnect),

    wait_for_offline_event(SubOfflineClientId, 500),

    %% now let's publish
    PubFun
        = fun(Socket, Mid) ->
                  Publish = mqtt5_v4compat:gen_publish("shared_sub_topic", 1,
                                                       <<Mid:8, (vmq_test_utils:rand_bytes(10))/binary>>,
                                               [{mid, Mid}], Cfg),
                  Puback = mqtt5_v4compat:gen_puback(Mid, Cfg),
                  ok = gen_tcp:send(Socket, Publish),
                  ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Cfg),
                  {Mid, Publish}
          end,
    Published = [PubFun(PubSocket, Mid) || Mid <- lists:seq(1,10)],
    %% since all messages should end up with the online subscriber, we
    %% can check they arrived one by one in the publish order.
    [begin
         ok = mqtt5_v4compat:expect_packet(SubSocketOnline, "publish", Expect, Cfg),
         Puback = mqtt5_v4compat:gen_puback(Mid, Cfg),
         ok = gen_tcp:send(SubSocketOnline, Puback)
     end || {Mid, Expect} <- Published ],
    disable_on_publish(),
    disable_on_subscribe().

message_expiry_interval(_) ->
    %% If the Message Expiry Interval has passed and the Server has
    %% not managed to start onward delivery to a matching subscriber,
    %% then it MUST delete the copy of the message for that subscriber
    %% [MQTT-3.3.2-5].

    %% The PUBLISH packet sent to a Client by the Server MUST contain
    %% a Message Expiry Interval set to thereceived value minus the
    %% time that the Application Message has been waiting in the
    %% Server [MQTT-3.3.2-6].
    enable_on_publish(),
    enable_on_subscribe(),
    enable_on_message_drop(),

    %% set up subscriber
    SubConnect = packetv5:gen_connect("message-expiry-sub", [{keepalive, 60},
                                                             {clean_start, false},
                                                             {properties, #{p_session_expiry_interval => 16#FFFFFFFF}}]),
    Connack = packetv5:gen_connack(?M5_CONNACK_ACCEPT),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),
    SubTopic60s = packetv5:gen_subtopic(<<"message/expiry/60s">>, 1),
    SubTopic1s = packetv5:gen_subtopic(<<"message/expiry/1s">>, 1),
    SubscribeAll = packetv5:gen_subscribe(10, [SubTopic60s, SubTopic1s], #{}),
    ok = gen_tcp:send(SubSocket, SubscribeAll),

    SubAck = packetv5:gen_suback(10, [1,1], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),
    Disconnect = packetv5:gen_disconnect(),
    ok = gen_tcp:send(SubSocket, Disconnect),
    ok = gen_tcp:close(SubSocket),

    %% set up publisher
    PubConnect = packetv5:gen_connect("message-expiry-pub", [{keepalive, 60}]),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, Connack, []),

    %% Publish some messages
    Expiry60s = #{p_message_expiry_interval => 60},
    PE60s = packetv5:gen_publish(<<"message/expiry/60s">>, 1, <<"e60s">>,
                                 [{properties, Expiry60s}, {mid, 0}]),
    ok = gen_tcp:send(PubSocket, PE60s),
    Puback0 = packetv5:gen_puback(0),
    ok = packetv5:expect_frame(PubSocket, Puback0),

    Expiry1s = #{p_message_expiry_interval => 1},
    PE1s = packetv5:gen_publish(<<"message/expiry/1s">>, 1, <<"e1s">>,
                                [{properties, Expiry1s}, {mid, 1}]),
    ok = gen_tcp:send(PubSocket, PE1s),
    Puback1 = packetv5:gen_puback(1),
    ok = packetv5:expect_frame(PubSocket, Puback1),
    ok = gen_tcp:close(PubSocket),

    %% Wait a bit to ensure the messages will have been held long
    %% enough in the queue of the offline session
    timer:sleep(1100),

    %% reconnect subscriber
    ConnackSP = packetv5:gen_connack(1, ?M5_CONNACK_ACCEPT),
    {ok, SubSocket1} = packetv5:do_client_connect(SubConnect, ConnackSP, []),

    %% receive the message with a long expiry interval
    {ok, RPE60s, <<>>} = packetv5:receive_frame(SubSocket1),
    #mqtt5_publish{topic = [<<"message">>, <<"expiry">>, <<"60s">>],
                   qos = 1,
                   properties = #{p_message_expiry_interval := Remaining}} = RPE60s,
    true = Remaining < 60,

    %% The 1s message shouldn't arrive, but let's just block a bit to
    %% make sure.
    {error, timeout} = gen_tcp:recv(SubSocket1, 0, 500),

    %% check that a message was really expired:
    1 = vmq_metrics:counter_val(queue_message_expired),

    ok = gen_tcp:close(SubSocket1),

    disable_on_publish(),
    disable_on_subscribe(),
    disable_on_message_drop().

publish_c2b_topic_alias(_Config) ->
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, _} = vmq_server_cmd:set_config(topic_alias_max_client, 3),

    Topic = "alias/topic",

    %% setup the subscriber
    SubConnect = packetv5:gen_connect("publish-c2b-topic-alias-subscriber", [{keepalive, 60}]),
    SubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{p_topic_alias_max => 3}),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, SubConnack, []),
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)], #{}),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),

    PubConnect = packetv5:gen_connect("publish-c2b-topic-alias", [{keepalive, 60}]),
    PubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{p_topic_alias_max => 3}),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, PubConnack, []),

    TA1 = #{p_topic_alias => 1},
    TASetup = packetv5:gen_publish(Topic, 0, <<"publish alias setup">>,
                                   [{properties, TA1}]),
    TAPub = packetv5:gen_publish(<<>>, 0, <<"publish alias pub">>,
                                 [{properties, TA1}]),

    ok = gen_tcp:send(PubSocket, TASetup),
    ok = gen_tcp:send(PubSocket, TAPub),

    ExpectSetupPub = packetv5:gen_publish(Topic, 0, <<"publish alias setup">>, []),
    ExpectTAPub = packetv5:gen_publish(Topic, 0, <<"publish alias pub">>, []),

    ok = packetv5:expect_frame(SubSocket, ExpectSetupPub),
    ok = packetv5:expect_frame(SubSocket, ExpectTAPub),

    ok = gen_tcp:close(PubSocket),
    ok = gen_tcp:close(SubSocket),

    disable_on_publish(),
    disable_on_subscribe(),
    ok.

publish_b2c_topic_alias(Config) ->
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, _} = vmq_server_cmd:set_config(topic_alias_max_broker, 3),

    SubClientId = vmq_cth:ustr(Config) ++ "-sub",
    PubClientId = vmq_cth:ustr(Config) ++ "-pub",
    Topic = list_to_binary(vmq_cth:utopic(Config)),

    SubConnect = packetv5:gen_connect(SubClientId,
                                      [{keepalive, 60}, {properties, #{p_topic_alias_max => 3}}]),
    SubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, SubConnack, []),
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)], #{}),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),

    PubConnect = packetv5:gen_connect(PubClientId, [{keepalive, 60}]),
    PubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, PubConnack, []),

    PubSetup = packetv5:gen_publish(Topic, 0, <<"publish alias setup">>, []),
    Pub = packetv5:gen_publish(Topic, 0, <<"publish alias pub">>, []),

    ok = gen_tcp:send(PubSocket, PubSetup),
    ok = gen_tcp:send(PubSocket, Pub),

    TA1 = #{p_topic_alias => 1},

    ExpectSetupPub = packetv5:gen_publish(Topic, 0, <<"publish alias setup">>,
                                          [{properties, TA1}]),
    ExpectTAPub = packetv5:gen_publish(<<>>, 0, <<"publish alias pub">>,
                                          [{properties, TA1}]),

    ok = packetv5:expect_frame(SubSocket, ExpectSetupPub),
    ok = packetv5:expect_frame(SubSocket, ExpectTAPub),

    ok = gen_tcp:close(PubSocket),
    ok = gen_tcp:close(SubSocket),

    disable_on_publish(),
    disable_on_subscribe(),
    ok.

forward_properties(_Config) ->

    enable_on_publish(),
    enable_on_subscribe(),

    Topic = "property/passthrough/test",

    %% setup subscriber
    SubConnect = packetv5:gen_connect("property-passthrough-sub-test", [{keepalive, 60}]),
    SubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, SubConnack, []),
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)], #{}),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),

    %% setup publisher
    Properties =
        [
         %% A Server MUST send the Payload Format Indicator unaltered to
         %% all subscribers receiving the Application Message
         %% [MQTT-3.3.2-4]
         #{p_payload_format_indicator => utf8},
         #{p_payload_format_indicator => unspecified},

         %% The Server MUST send the Response Topic unaltered to all
         %% subscribers receiving the Application Message
         %% [MQTT-3.3.2-15].
         #{p_response_topic => <<"response topic">>},

         %% The Server MUST send the Correlation Data unaltered to all
         %% subscribers receiving the Application Message
         %% [MQTT-3.3.2-16].
         #{p_correlation_data => <<"correlation data">>},

         %% The Server MUST send all User Properties unaltered in a
         %% PUBLISH packet when forwarding the Application Message to
         %% a Client [MQTT-3.3.2-17]. The Server MUST maintain the
         %% order of User Properties when forwarding the Application
         %% Message [MQTT-3.3.2-18].
         #{p_user_property => [{<<"k1">>, <<"v1">>},
                               {<<"k2">>, <<"v2">>},
                               {<<"k3">>, <<"v3">>},
                               {<<"k2">>, <<"v4">>}]},

         %% A Server MUST send the Content Type unaltered to all
         %% subscribers receiving the Application Message
         %% [MQTT-3.3.2-20].
         #{p_content_type => <<"content type">>}],

    PubConnect = packetv5:gen_connect("property-passthrough-pub-test", [{keepalive, 60}]),
    PubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, PubConnack, []),

    lists:foreach(
      fun(Property) ->
              ct:pal("Testing property: ~p", [Property]),
              Pub = packetv5:gen_publish(Topic, 0, <<"message">>,
                                         [{properties, Property}]),
              ok = gen_tcp:send(PubSocket, Pub),
              ok = packetv5:expect_frame(SubSocket, Pub)
      end, Properties),

    disable_on_publish(),
    disable_on_subscribe().

max_packet_size(Config) ->
    enable_on_publish(),
    enable_on_subscribe(),
    enable_on_message_drop(),
    SubClientId = vmq_cth:ustr(Config) ++ "-sub",
    PubClientId = vmq_cth:ustr(Config) ++ "-pub",
    Topic = list_to_binary(vmq_cth:utopic(Config)),
    Pub = packetv5:gen_publish(Topic, 0, <<"publish">>, [{properties,
                                                          #{p_user_property => [{<<"hello">>, <<"world">>}]}}]),
    ReducedPub = packetv5:gen_publish(Topic, 0, <<"publish">>, []),
    TooLargePub = packetv5:gen_publish(Topic, 0, <<"large enough to be discarded publish">>, []),
    SubConnect = packetv5:gen_connect(SubClientId,
                                      [{keepalive, 60}, {properties, #{p_max_packet_size => iolist_size(ReducedPub)}}]),
    SubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, SubConnack, []),
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)], #{}),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),
    PubConnect = packetv5:gen_connect(PubClientId, [{keepalive, 60}]),
    PubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, PubConnack, []),

    % checks that the user property of a publish message is stripped in
    % case the message would be larger than specified maximum packet
    % size [MQTT-3.4.2-3]
    ok = gen_tcp:send(PubSocket, Pub),
    ok = packetv5:expect_frame(SubSocket, ReducedPub),

    % checks that the message is discarded in case the message would be
    % larger than specified maximum packet size [MQTT-3.1.2-25]
    ok = gen_tcp:send(PubSocket, TooLargePub),
    {error, timeout} = gen_tcp:recv(SubSocket, 0, 1000),

    ok = gen_tcp:close(PubSocket),
    ok = gen_tcp:close(SubSocket),
    disable_on_publish(),
    disable_on_subscribe(),
    disable_on_message_drop(),
    ok.

direct_plugin_exports_test(Cfg) ->
    Topic = vmq_cth:utopic(Cfg),
    WTopic = re:split(list_to_binary(Topic), <<"/">>),
    {RegFun0, PubFun3, {SubFun1, UnsubFun1}}
        = vmq_reg:direct_plugin_exports(?FUNCTION_NAME),
    ok = RegFun0(),
    {ok, [0]} = SubFun1(WTopic),
    %% this client-id generation is taken from
    %% `vmq_reg:direct_plugin_exports/1`. It would be better if that
    %% function would return the generated client-id.
    ClientId = fun(T) ->
                       list_to_binary(
                         base64:encode_to_string(
                           integer_to_binary(
                             erlang:phash2(T)
                            )
                          ))
               end,
    TestSub =
        fun(T, MustBePresent) ->
                Subscribers = vmq_reg_trie:fold({"", ClientId(self())},
                                                T, fun(E,_From,Acc) -> [E|Acc] end, []),
                IsPresent = lists:member({{"", ClientId(self())}, 0}, Subscribers),
                MustBePresent =:= IsPresent
        end,
    vmq_cluster_test_utils:wait_until(fun() -> TestSub(WTopic, true) end, 100, 10),
    {ok, {1, 0}} = PubFun3(WTopic, <<"msg1">>, #{}),
    receive
        {deliver, WTopic, <<"msg1">>, 0, false, false} -> ok;
        Other -> throw({received_unexpected_msg, Other})
    after
        1000 ->
            throw(didnt_receive_expected_msg_from_direct_plugin_exports)
    end,
    ok = UnsubFun1(WTopic),
    vmq_cluster_test_utils:wait_until(fun() -> TestSub(WTopic, false) end, 100, 10),
    {ok, {0, 0}} = PubFun3(WTopic, <<"msg2">>, #{}),
    receive
        M -> throw({received_unexpected_msg_from_direct_plugin_exports, M})
    after
        100 -> ok
    end.

%% publish_c2b_invalid_topic_alias(Config) ->
%%     vmq_server_cmd:set_config(topic_alias_max_client, 10),
%%     %% The Client MUST NOT send a Topic Alias in a PUBLISH packet to
%%     %% the Server greater than this value [MQTT-3.2.2-17].

%%     vmq_server_cmd:set_config(topic_alias_max_client, 0),
%%     %% Topic Alias Maximum is absent or 0, the Client MUST NOT send
%%     %% any Topic Aliases on to the Server [MQTT-3.2.2-18].
%%     ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_, _, _) -> ok.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_on_message_drop(_, Promise, max_packet_size_exceeded) ->
    {_QoS, _Topic, <<"large enough to be discarded publish">> = _Payload, _Props} = Promise(),
    ok;
hook_on_message_drop({"", <<"message-expiry-sub">>}, _, expired) -> ok.

hook_on_client_offline(SubscriberId) ->
    ?CLIENT_OFFLINE_EVENT_SRV ! {on_client_offline, SubscriberId}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_on_subscribe() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                     convert, 4}}]).
enable_on_publish() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                    convert, 7}}]).
enable_on_message_drop() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           on_message_drop, ?MODULE, hook_on_message_drop, 3).

disable_on_subscribe() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                      convert, 4}}]).
disable_on_publish() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                      convert, 7}}]).
disable_on_message_drop() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           on_message_drop, ?MODULE, hook_on_message_drop, 3).


helper_pub_qos1(ClientId, Mid, Publish, Config) ->
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Puback = mqtt5_v4compat:gen_puback(Mid, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Config),
    gen_tcp:close(Socket).

helper_pub_qos2(ClientId, Mid, Publish, Config) ->
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(Mid, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(Mid, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(Mid, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubcomp", Pubcomp, Config),
    gen_tcp:close(Socket).

helper_pattern_matching(PubTopic, Config) ->
    Connect = mqtt5_v4compat:gen_connect("test-helper", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish(PubTopic, 0, <<"message">>, [{retain, true}], Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, mqtt5_v4compat:gen_disconnect(Config)),
    gen_tcp:close(Socket).

expect_alive(Socket) ->
    Pingreq = packet:gen_pingreq(),
    Pingresp = packet:gen_pingresp(),
    ok = gen_tcp:send(Socket, Pingreq),
    ok = packet:expect_packet(Socket, "pingresp", Pingresp).

wait_for_offline_event(ClientId, Timeout) ->
    ClientIdBin =
        case is_list(ClientId) of
            true ->
                list_to_binary(ClientId);
            false -> ClientId
        end,
    receive
        {on_client_offline, {"", ClientIdBin}} ->
            ok
    after
        Timeout ->
            throw(client_not_offline)
    end.

start_client_offline_events(Cfg) ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           on_client_offline, ?MODULE, hook_on_client_offline, 1),
    TestPid = self(),
    F = fun(Fun) ->
                receive
                    {on_client_offline, _} = E ->
                        TestPid ! E,
                        Fun(Fun);
                    {stop, Ref} ->
                        TestPid ! {ok, Ref},
                        ok
                end
        end,
    EventProc = spawn_link(fun() -> F(F) end),
    true = register(?CLIENT_OFFLINE_EVENT_SRV, EventProc),
    [{?CLIENT_OFFLINE_EVENT_SRV, EventProc}|Cfg].


stop_client_offline_events(Cfg) ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           on_client_offline, ?MODULE, hook_on_client_offline, 1),
    Pid = proplists:get_value(?CLIENT_OFFLINE_EVENT_SRV, Cfg),
    Ref = make_ref(),
    Pid ! {stop, Ref},
    receive
        {ok, Ref} ->
            ok
    after
        1000 ->
            throw({could_not_stop, ?CLIENT_OFFLINE_EVENT_SRV})
    end.
