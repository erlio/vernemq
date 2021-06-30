-module(vmq_queue_SUITE).
-include("src/vmq_server.hrl").

-ifdef(nowarn_gen_fsm).
-compile([{nowarn_deprecated_function,
          [{gen_fsm,sync_send_all_state_event,2}]}]).
-endif.

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([queue_crash_test/1,
         queue_fifo_test/1,
         queue_lifo_test/1,
         queue_fifo_offline_drop_test/1,
         queue_lifo_offline_drop_test/1,
         queue_offline_transition_test/1,
         queue_persistent_client_expiration_test/1,
         queue_force_disconnect_test/1,
         queue_force_disconnect_cleanup_test/1]).

-export([hook_auth_on_publish/6,
         hook_auth_on_subscribe/3,
         hook_on_message_drop/3]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_config:set_env(queue_deliver_mode, fanout, false),
    enable_hooks(),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [queue_crash_test,
     queue_fifo_test,
     queue_lifo_test,
     queue_fifo_offline_drop_test,
     queue_lifo_offline_drop_test,
     queue_offline_transition_test,
     queue_persistent_client_expiration_test,
     queue_force_disconnect_test,
     queue_force_disconnect_cleanup_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
queue_crash_test(_) ->
    Parent = self(),
    {_, ClientId} = SubscriberId = {"", <<"mock-client">>},
    QueueOpts = maps:merge(#{cleanup_on_disconnect => false}, vmq_queue:default_opts()),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid1}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId,
                                  [{[<<"test">>, <<"topic">>], 1}]),
    %% at this point we've a working subscription
    timer:sleep(10),
    Msg = msg([<<"test">>, <<"topic">>], <<"test-message">>, 1),
    {ok, {1, 0}} = vmq_reg:publish(true, vmq_reg_trie, ClientId, Msg),
    receive_msg(QPid1, 1, Msg),

    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),
    {offline, fanout, 0, 0, false} = vmq_queue:status(QPid1),

    %% fill the offline queue
    {ok, {1, 0}} = vmq_reg:publish(true, vmq_reg_trie, ClientId, Msg),
    {offline, fanout, 1, 0, false} = vmq_queue:status(QPid1),

    %% crash the queue
    catch gen_fsm:sync_send_all_state_event(QPid1, byebye),
    false = is_process_alive(QPid1),
    timer:sleep(10),
    NewQPid = vmq_reg:get_queue_pid(SubscriberId),
    {offline, fanout, 1, 0, false} = vmq_queue:status(NewQPid),

    %% reconnect
    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := NewQPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    receive_persisted_msg(NewQPid, 1, Msg),
    {online, fanout, 0, 1, false} = vmq_queue:status(NewQPid),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_fifo_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-fifo-client">>},
    QueueOpts = maps:merge(#{cleanup_on_disconnect => false}, vmq_queue:default_opts()),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId,
                           [{[<<"test">>, <<"fifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"fifo">>, <<"topic">>]),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),

    ok = receive_multi(QPid, 1, Msgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_lifo_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-lifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false, queue_type => lifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"lifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"lifo">>, <<"topic">>]),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),

    ok = receive_multi(QPid, 1, lists:reverse(Msgs)), %% reverse list to get lifo
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_fifo_offline_drop_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-fifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 10}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"fifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"fifo">>, <<"topic">>]), % publish 100, only the first 10 are kept
    {offline, fanout, 10, 0, false} = vmq_queue:status(QPid),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    {KeptMsgs, _} = lists:split(10, Msgs),
    ok = receive_multi(QPid, 1, KeptMsgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).


queue_lifo_offline_drop_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-lifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 10,
                                                       queue_type => lifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId,
                           [{[<<"test">>, <<"lifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"lifo">>, <<"topic">>]), % publish 100, only the first 10 are kept
    {offline, fanout, 10, 0, false} = vmq_queue:status(QPid),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    {KeptMsgs, _} = lists:split(10, lists:reverse(Msgs)),
    ok = receive_multi(QPid, 1, KeptMsgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).


queue_offline_transition_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-trans-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(10), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    SessionPid1 ! {go_down_in, 1},
    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"transition">>]), % publish 100

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    ok = receive_multi(QPid, 1, Msgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_persistent_client_expiration_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"persistent-client-expiration">>},
    %% Set the persistent client to expire after 15 seconds
    application:set_env(vmq_server, persistent_client_expiration, 2),
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(50), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    SessionPid1 ! {go_down_in, 1},
    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"transition">>]),
    NumPubbedMsgs = length(Msgs),

    timer:sleep(50), % give some time to plumtree
    {ok, FoundMsgs} = vmq_message_store:find(SubscriberId, other),
    NumPubbedMsgs = length(FoundMsgs),

    %% let's wait for the persistent-client-expiration to kick in
    timer:sleep(3000),

    not_found = vmq_queue_sup_sup:get_queue_pid(SubscriberId),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_force_disconnect_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"force-client-disconnect">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
           queue_pid := QPid0}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"disconnect">>], 1}]),
    timer:sleep(50), % give some time to plumtree

    monitor(process, SessionPid1),
    vmq_queue:force_disconnect(QPid0, ?ADMINISTRATIVE_ACTION),

    % ensure we got disconnected
    receive
        {'DOWN', _MRef, process, SessionPid1, _} -> ok
    end,

    % Reconnect and ensure SessionPresent, and same QueuePid
    {ok, #{session_present := true,
           queue_pid := QPid0}} = vmq_reg:register_subscriber_(Parent, SubscriberId, false, QueueOpts, 10).


queue_force_disconnect_cleanup_test(_) ->
    NonConsumingSessionPid = self(),
    SubscriberId = {"", <<"force-client-discleanup">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPresent = false,
    {ok, #{session_present := SessionPresent,
           queue_pid := QPid0}} = vmq_reg:register_subscriber_(NonConsumingSessionPid, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"discleanup">>], 1}]),
    timer:sleep(50), % give some time to plumtree

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"discleanup">>]),
    NumPubbedMsgs = length(Msgs),

    timer:sleep(50), % give some time to plumtree
    {ok, FoundMsgs} = vmq_message_store:find(SubscriberId, other),
    NumPubbedMsgs = length(FoundMsgs),

    vmq_queue:force_disconnect(QPid0, ?ADMINISTRATIVE_ACTION, true),

    % Ensure all Subscriptions are gone
    [] = vmq_reg:subscriptions_for_subscriber_id(SubscriberId),

    % SessionPresent should be again `false` and we should get a new Queue Pid
    {ok, #{session_present := SessionPresent,
           queue_pid := QPid1}} = vmq_reg:register_subscriber_(NonConsumingSessionPid, SubscriberId, false, QueueOpts, 10),
    true = (QPid0 =/= QPid1),
    false = is_process_alive(QPid0),

    {ok, []} = vmq_message_store:find(SubscriberId, other).

publish_multi({_, ClientId}, Topic) ->
    publish_multi(ClientId, Topic, []).

publish_multi(ClientId, Topic, Acc) when length(Acc) < 100 ->
    Msg = msg(Topic, list_to_binary("test-message-"++ integer_to_list(length(Acc))), 1),
    {ok, {1, 0}} = vmq_reg:publish(true, vmq_reg_trie, ClientId, Msg),
    publish_multi(ClientId, Topic, [Msg|Acc]);
publish_multi(_, _, Acc) -> lists:reverse(Acc).

receive_multi(QPid, QoS, Msgs) ->
    PMsgs = [#deliver{qos=QoS, msg=Msg#vmq_msg{persisted=true, qos=1}} || Msg <- Msgs],
    receive_multi(QPid, PMsgs).

receive_multi(_, []) -> ok;
receive_multi(QPid, Msgs) ->
    receive
        {received, QPid, RecMsgs} ->
            case lists:split(length(RecMsgs), Msgs) of
                {RecMsgs, RestMsgs} ->
                    receive_multi(QPid, RestMsgs);
                _ ->
                    exit({wrong_messages, {RecMsgs, Msgs}})
            end;
        M ->
            exit({wrong_message, M})
    end.

mock_session(Parent) ->
    receive
        {to_session_fsm, {mail, QPid, new_data}} ->
            vmq_queue:active(QPid),
            mock_session(Parent);
        {to_session_fsm, {mail, QPid, Msgs, _, _}} ->
            vmq_queue:notify(QPid),
            release_msgs(QPid, Msgs),
            timer:sleep(100),
            Parent ! {received, QPid, Msgs},
            mock_session(Parent);
        {go_down_in, Ms} ->
            timer:sleep(Ms);
        _ -> % go down
            ok
    end.

release_msgs(_, []) -> ok;
release_msgs(QPid, [Msg | Rest]) ->
    vmq_queue:release_message(QPid, Msg),
    release_msgs(QPid, Rest).

msg(Topic, Payload, QoS) ->
    #vmq_msg{msg_ref=vmq_mqtt_fsm_util:msg_ref(),
             mountpoint="",
             routing_key=Topic,
             payload=Payload,
             qos=QoS,
             properties=#{}}.

receive_msg(QPid, QoS, Msg) ->
    %% if we were able to persist the message
    %% we'll set the persist flag
    PMsg = Msg#vmq_msg{persisted=true},
    receive
        {received, QPid, [#deliver{qos=QoS, msg=PMsg}]} ->
            ok;
        M ->
            exit({wrong_message, M})
    end.

receive_persisted_msg(QPid, QoS, Msg) ->
    %% if we were able to persist the message
    %% we'll set the persist flag,
    %% BUT we've also set the qos of the message
    %% to the one of the subscription
    PMsg = Msg#vmq_msg{persisted=true, qos=QoS},
    receive
        {received, QPid, [#deliver{qos=QoS, msg=PMsg}]} ->
            ok;
        M ->
            exit({wrong_message, M})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_hooks() ->
    vmq_plugin_mgr:enable_module_plugin(auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    vmq_plugin_mgr:enable_module_plugin(auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    vmq_plugin_mgr:enable_module_plugin(on_message_drop, ?MODULE, hook_on_message_drop, 3).

hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_auth_on_subscribe(_, _, _) -> ok.
hook_on_message_drop(_, _, queue_full) -> ok.
