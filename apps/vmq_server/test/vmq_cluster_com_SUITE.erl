-module(vmq_cluster_com_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ct:log("node name ~p", [node()]),
    Node = vmq_cluster_test_utils:start_node(test_com1, Config, default_case),
    ct:pal("This is the default NODE : ~p~n", [Node]),
    {ok, _} = ct_cover:add_nodes([Node]),
    vmq_cluster_test_utils:wait_until_ready([Node]),
    [{node, Node}, S|Config].

end_per_suite(_Config) ->
    ct_slave:stop(test_com1),
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    ClusterNodePid = setup_mock_vmq_cluster_node(Config),
    [{cluster_node_pid, ClusterNodePid}|Config].

end_per_testcase(_Case, Config) ->
    terminate_mock_vmq_cluster_node(Config),
    ok.

all() ->
    [
     connect_success_and_send_message,
     reconnect_resends_backup_on_ack_timeout,
     msgs_buffered_while_unreachable,
     reconnect_resends_backup_and_next_buffered_messages,
     msgs_dropped_when_exceeding_configured_buffer_size
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_params(_RemoteNode) ->
    {gen_tcp, {127,0,0,1}, 12345}.

connect_success_and_send_message(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket, Config),

    % send test message
    ok = send_message(ClusterNodePid, hello_world),
    % recv and ack this message
    ok = recv_and_ack_message_block(Socket, ClusterNodePid, hello_world).

reconnect_resends_backup_on_ack_timeout(Config) ->
    % check that message in transit is not lost.
    % After e.g. a receiver crash, the sender reconnects and retransmits the last unacked block
    % We simulate the crash by consuming the message but not acknowledging it to the sender.
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),

    % send test message
    ok = send_message(ClusterNodePid, hello_world),
    % recv this message without acknowledging to sender (=simulating receiver crash)
    ok = recv_message_block(Socket1, ClusterNodePid, hello_world),

    % sender closed connection after ack_timeout
    {error, closed} = gen_tcp:recv(Socket1, 0),
    % sender reconnects
    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),

    % sender retransmits the previously unacked message
    % This time we recv and ack the original message
    ok = recv_and_ack_message_block(Socket2, ClusterNodePid, hello_world).


msgs_buffered_while_unreachable(Config) ->
    % check that message isn't lost
    % In case the receiver is unreachable incoming messages are buffered
    % and delivered upon reconnect.
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),
    % close this socket
    gen_tcp:close(Socket1),
    % send test message, will be buffered and delivered on next successful reconnect
    ok = send_message(ClusterNodePid, hello_world),
    
    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),

    % recv and ack the buffered message
    ok = recv_and_ack_message_block(Socket2, ClusterNodePid, hello_world).

reconnect_resends_backup_and_next_buffered_messages(Config) ->
    % check that after reconnect (e.g. after receiver crash as simulated here),
    % the sender retransmits the backup + next buffered messages.
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),

    % send 2 messages
    ok = send_message(ClusterNodePid, hello_world_1),
    ok = send_message(ClusterNodePid, hello_world_2),

    % recv first message without acknowledging to sender (=simulating receiver crash)
    ok = recv_message_block(Socket1, ClusterNodePid, hello_world_1),

    % send 2 more messages
    ok = send_message(ClusterNodePid, hello_world_3),
    ok = send_message(ClusterNodePid, hello_world_4),

    % sender closed connection
    {error, closed} = gen_tcp:recv(Socket1, 0),
    % sender reconnects
    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),

    % sender retransmits the first message and the next buffered messages in one block
    % This time we recv and ack all 4 messages (in correct order)
    ok = recv_and_ack_message_block(Socket2, ClusterNodePid, [hello_world_1, hello_world_2, hello_world_3, hello_world_4]).

msgs_dropped_when_exceeding_configured_buffer_size(Config) ->
    % check that the max_queue_size limit is respected.
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),
    % close this socket
    gen_tcp:close(Socket1),

    % each of those messages results in a binary size of 250 bytes to be buffered :
    Msg1 = <<1:237/unit:8>>,
    Msg2 = <<2:237/unit:8>>,
    Msg3 = <<3:237/unit:8>>,
    Msg4 = <<4:237/unit:8>>,

    Msg5 = <<5:1/unit:8>>,

    % send test messages, will be buffered until configured buffer size reached
    ok = send_message(ClusterNodePid, Msg1),
    ok = send_message(ClusterNodePid, Msg2),
    ok = send_message(ClusterNodePid, Msg3),
    ok = send_message(ClusterNodePid, Msg4),
  
    % outgoing_clustering_buffer_size was configured to 1000 bytes
    % at this point this size is exactly reached hence the next small message will be dropped:
    Msg5 = <<5:1/unit:8>>,
    {error, msg_dropped} = send_message(ClusterNodePid, Msg5),

    % reconnect
    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),

    % recv and ack the 4 buffered messages
    ok = recv_and_ack_message_block(Socket2, ClusterNodePid, [Msg1,Msg2,Msg3,Msg4]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



send_until_tcp_buffer_full(ClusterNodePid) ->
   send_until_tcp_buffer_full(ClusterNodePid, 0).
send_until_tcp_buffer_full(ClusterNodePid, MsgsAcc) ->
    % the only way we detect that the buffer is full is that vmq_cluster_node will close
    % the connection and will reconnect
    case send_message(ClusterNodePid, <<1:10000, MsgsAcc:32>>) of
        ok  ->
            send_until_tcp_buffer_full(ClusterNodePid, MsgsAcc + 1);
        {error, msg_dropped} ->
            MsgsAcc - 1
    end.

recv_until_tcp_buffer_empty(Socket, N) ->
    recv_until_tcp_buffer_empty(Socket, 0, N).

recv_until_tcp_buffer_empty(Socket, I, N) when I =< N ->
    recv_message(Socket, <<1:10000, I:32>>),
    recv_until_tcp_buffer_empty(Socket, I + 1, N);
recv_until_tcp_buffer_empty(_, _, _) -> ok.


setup_mock_vmq_cluster_node(Config) ->
    setup_mock_vmq_cluster_node(Config, []).

setup_mock_vmq_cluster_node(Config, Opts) ->
    Node = proplists:get_value(node, Config),
    % make the test_com1 node connect to myself
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_opts, Opts, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_params_module, ?MODULE, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_timeout, 1000, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_clustering_buffer_size, 1000, false]),
    {ok, ClusterNodePid} = rpc:block_call(Node, vmq_cluster_node, start_link, [node()]),
    ClusterNodePid.

terminate_mock_vmq_cluster_node(Config) ->
    Node = proplists:get_value(node, Config),
    ClusterNodePid = cluster_node_pid(Config),
    rpc:block_call(Node, erlang, exit, [ClusterNodePid, kill]).

cluster_node_pid(Config) ->
    proplists:get_value(cluster_node_pid, Config).

recv_connect(Socket, Config) ->
    Node = proplists:get_value(node, Config),
    NodeName = term_to_binary(Node),
    L1 = byte_size(NodeName),
    HandshakeMsg = <<"vmq-connect", L1:32, NodeName/binary>>,
    {ok, HandshakeMsg} = gen_tcp:recv(Socket, byte_size(HandshakeMsg)),
    ok.


send_message(ClusterNodePid, Msg) ->
    rpc:call(node(ClusterNodePid), vmq_cluster_node, publish, [ClusterNodePid, Msg]).

recv_message(Socket, Term) ->
    TermBin = term_to_binary(Term),
    L = byte_size(TermBin),
    Msg = <<"msg", L:32, TermBin/binary>>,
    BatchMsg = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    case gen_tcp:recv(Socket, byte_size(BatchMsg)) of
        {ok, BatchMsg} -> ok;
        E ->
            io:format(user, "got ~p instead of ~p~n", [E, {ok, BatchMsg}]),
            E
    end.

recv_and_ack_message_block(Socket, ClusterNodePid, Msgs) ->
    ok = recv_message_block(Socket, ClusterNodePid, Msgs),
    ack_message_block(ClusterNodePid).

recv_message_block(Socket, ClusterNodePid, Msgs) when is_list(Msgs) ->
    %%Block = iolist_to_binary(wrap_msgs(Msgs)),
    Block = wrap_msgs(Msgs),
    Tail = tail(ClusterNodePid),
     L = iolist_size(Block),
    BatchMsg = iolist_to_binary([<<"vmq-send", L:32>>, Block, Tail]),
    %%BatchMsg = <<"vmq-send", L:32, Block/binary, Tail/binary>>,
    case gen_tcp:recv(Socket, byte_size(BatchMsg)) of
        {ok, BatchMsg} -> ok;
        E ->
            io:format(user, "got ~p instead of ~p~n", [E, {ok, BatchMsg}]),
            E
    end;
recv_message_block(Socket, ClusterNodePid, Msg) ->
    recv_message_block(Socket, ClusterNodePid, [Msg]).

ack_message_block(ClusterNodePid) ->
    ClusterNodePid ! block_ack,
    ok.

wrap_msgs(Msgs) ->
    wrap_msgs_(Msgs, []).

wrap_msgs_([], Acc) ->
    lists:reverse(Acc);
wrap_msgs_([Msg | Rest], Acc) ->
    Bin = term_to_binary(Msg),
    L = byte_size(Bin),
    Wrap = <<"msg", L:32, Bin/binary>>,
    wrap_msgs_(Rest, [Wrap | Acc]).

tail(Pid) ->
   Bin = term_to_binary(Pid),
   L = byte_size(Bin),
   [<<"vmq-tail", L:32>>, Bin].
