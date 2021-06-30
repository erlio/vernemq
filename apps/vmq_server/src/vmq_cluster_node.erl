%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
-module(vmq_cluster_node).
-include("vmq_server.hrl").

%% API
-export([start_link/1,
         publish/2,
         enqueue/4,
         enqueue_async/3,
         connect_params/1,
         status/1]).

%% gen_server callbacks
-export([init/1,
         loop/1]).

-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-record(state, {
          parent,
          node,
          socket,
          transport,
          reachable=false,
          waiting_for_ack=false,
          pending = [],
          transit = [],
          max_queue_size,
          reconnect_tref,
          waiting_ack_tref,
          async_connect_pid,
          bytes_dropped={os:timestamp(), 0},
          bytes_send={os:timestamp(), 0}}).

-define(RECONNECT, 1000).
-define(ACK_TIMEOUT, 3000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RemoteNode) ->
    proc_lib:start_link(?MODULE, init, [[self(), RemoteNode]]).

publish(Pid, Msg) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {msg, self(), Ref, Msg},
    receive
        {Ref, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

enqueue(Pid, Term, BufferIfUnreachable, Timeout) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {enq, self(), Ref, Term, BufferIfUnreachable},
    case Timeout of
        infinity ->
            receive
                {Ref, Reply} ->
                    demonitor(MRef, [flush]),
                    Reply;
                {'DOWN', MRef, process, Pid, Reason} ->
                    {error, Reason}
            end;
        _ ->
            receive
                {Ref, Reply} ->
                    demonitor(MRef, [flush]),
                    Reply;
                {'DOWN', MRef, process, Pid, Reason} ->
                    {error, Reason}
            after
                Timeout ->
                    demonitor(MRef, [flush]),
                    {error, timeout}
            end
    end.

enqueue_async(Pid, Term, BufferIfUnreachable) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {enq, self(), Ref, Term, BufferIfUnreachable},
    {MRef, Ref}.

status(Pid) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {status, self(), Ref},
    receive
        {Ref, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

init([Parent, RemoteNode]) ->
    MaxQueueSize = vmq_config:get_env(outgoing_clustering_buffer_size),
    proc_lib:init_ack(Parent, {ok, self()}),
    % Delay the initial connect attempt, this is useful when automating
    % cluster node setup, where multiple nodes are concurrently setup.
    % Without a delay a node may try to connect to a cluster node that
    % hasn't finished setting up the vmq cluster listener.
    erlang:send_after(1000, self(), reconnect),
    loop(#state{parent=Parent, node=RemoteNode, max_queue_size=MaxQueueSize}).

loop(#state{pending=Pending, waiting_for_ack=Waiting, reachable=Reachable} = State)
  when
      Pending == [];
      Waiting == true;
      Reachable == false ->
    receive
        M ->
            loop(handle_message(M, State))
    end;
loop(#state{} = State) ->
    receive
        M ->
            loop(handle_message(M, State))
    after
        0 ->
            loop(internal_flush(State))
    end.

buffer_message(BinMsg, #state{pending=Pending, max_queue_size=Max,
                              reachable=Reachable,
                              bytes_dropped={{M, S, _}, V}} = State) ->
    {NewPending, Dropped} =
    case Reachable of
        true ->
            {[BinMsg|Pending], 0};
        false ->
            case iolist_size(Pending) < Max of
                true ->
                    {[BinMsg|Pending], 0};
                false ->
                    {Pending, byte_size(BinMsg)}
            end
    end,
    NewBytesDropped =
    case os:timestamp() of
        {M, S, _} = TS ->
            {TS, V + Dropped};
        TS ->
            _ = vmq_metrics:incr_cluster_bytes_dropped(V + Dropped),
            {TS, 0}
    end,
    {Dropped, maybe_flush(State#state{pending=NewPending, bytes_dropped=NewBytesDropped})}.

handle_message({enq, CallerPid, Ref, _, BufferIfUnreachable},
               #state{reachable=false} = State)
  when BufferIfUnreachable =:= false ->
    CallerPid ! {Ref, {error, not_reachable}},
    State;
handle_message({enq, CallerPid, Ref, Term, _}, State) ->
    Bin = term_to_binary({CallerPid, Ref, Term}),
    L = byte_size(Bin),
    BinMsg = <<"enq", L:32, Bin/binary>>,
    %% buffering is allowed, but will only happen if the remote node
    %% is unreachable
    {Dropped, NewState} = buffer_message(BinMsg, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}};
        false ->
            %% reply directly from other node
            ignore
    end,
    NewState;
handle_message({msg, CallerPid, Ref, Msg}, State) ->
    Bin = term_to_binary(Msg),
    L = byte_size(Bin),
    BinMsg = <<"msg", L:32, Bin/binary>>,
    {Dropped, NewState} = buffer_message(BinMsg, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}};
        false ->
            CallerPid ! {Ref, ok}
    end,
    NewState;
handle_message({connect_async_done, AsyncPid, {ok, {Transport, Socket}}},
               #state{async_connect_pid=AsyncPid, node=RemoteNode} = State) ->
    NodeName = term_to_binary(node()),
    L = byte_size(NodeName),
    Msg = [<<"vmq-connect">>, <<L:32, NodeName/binary>>],
    case send(Transport, Socket, Msg) of
        ok ->
            lager:info("cluster_node ~p : successfully connected to cluster node ~p", [node(), RemoteNode]),
            NewState = State#state{socket=Socket, transport=Transport,
                        %% !!! remote node is reachable
                        async_connect_pid=undefined,
                        reachable=true,
                        waiting_for_ack=false},
            internal_flush(NewState);
        {error, Reason} ->
            lager:warning("cluster_node ~p : can't initiate connect to cluster node ~p due to ~p", [node(), RemoteNode, Reason]),
            close_reconnect(State)
    end;
handle_message({connect_async_done, AsyncPid, error}, #state{async_connect_pid=AsyncPid} = State) ->
    % connect_async already logged the error details
    close_reconnect(State);
handle_message(block_ack, State) ->
     %% block ack received, cancel timeout timer
     NewState = cancel_ack_timer(State),
     NewState#state{waiting_for_ack=false, transit=[]};
handle_message(reconnect, #state{reachable=false} = State) ->
    %% avoid timer interference
    NewState = cancel_ack_timer(State),
    connect(NewState#state{reconnect_tref=undefined});
handle_message(reconnect, #state{reachable=true} = State) ->
    %% avoid timer interference
    NewState = cancel_ack_timer(State),
    close_reconnect(NewState);
handle_message({status, CallerPid, Ref}, #state{socket=Socket, reachable=Reachable}=State) ->
    Status =
    case Reachable of
        true ->
            up;
        false when Socket == undefined ->
            init;
        false ->
            down
    end,
    CallerPid ! {Ref, Status},
    State;
handle_message({system, From, Request}, #state{parent=Parent}= State) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
handle_message({NetEvClosed, Socket}, #state{node=RemoteNode, socket=Socket} = State)
  when NetEvClosed == tcp_closed;
       NetEvClosed == ssl_closed ->
    lager:warning("connection to node ~p has been closed, reconnect in ~pms",
                  [RemoteNode, ?RECONNECT]),
    close_reconnect(State);
handle_message({NetEvError, Socket, Reason}, #state{node=RemoteNode, socket=Socket} = State)
  when NetEvError == tcp_error;
       NetEvError == ssl_error ->
    lager:warning("connection to node ~p has been closed due to error ~p, reconnect in ~pms",
                  [RemoteNode, Reason, ?RECONNECT]),
    close_reconnect(State);

handle_message(Msg, #state{node=Node, reachable=Reachable} = State) ->
    lager:warning("got unknown message ~p for node ~p (reachable ~p)",
                  [Msg, Node, Reachable]),
    State.

-define(FLUSH_THRESHOLD, 1460). % tcp-over-ethernet MSS 1460
maybe_flush(#state{pending=Pending} = State) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true ->
            internal_flush(State);
        false ->
            State
    end.

%% In case the receiving node crashes while it still had messages in the receiving tcp buffer
%% those messages would be lost.
%% Therefore we introduce a handshake at application level:
%%  - append a 'tail' to the block of messages that are sent
%%  - keep a backup copy of the messages in a 'transit' buffer
%%  - await the block_ack from the receiving node, as it handled the complete block
%%    (upon receipt of the 'tail')
%%  - when the block_ack is received, the transit buffer is cleared
%%    and the next block can be transmitted
%% This handshake is protected by a timeout which will trigger a reconnect
%% Note that currently the backup is kept only in memory, a future improvement
%% would be to persist the backup on disk to also protect against message loss
%% in case the transmitting node crashes.
internal_flush(#state{reachable=false} = State) -> State;
internal_flush(#state{waiting_for_ack=true} = State) -> State;
internal_flush(#state{pending=[], transit=[]} = State) -> State;
internal_flush(#state{pending=Pending, transit=Backup, node=Node, transport=Transport,
                      socket=Socket, bytes_send={{M, S, _}, V}} = State) ->
    Transit = [Backup, lists:reverse(Pending)],
    L = iolist_size(Transit),
    BinPid = term_to_binary(self()),
    P = byte_size(BinPid),
    Msg = [<<"vmq-send", L:32>>, Transit, <<"vmq-tail", P:32>>, BinPid],
    case send(Transport, Socket, Msg) of
        ok ->
            NewBytesSend =
            case os:timestamp() of
                {M, S, _} = TS ->
                    {TS, V + L};
                TS ->
                    _ = vmq_metrics:incr_cluster_bytes_sent(V + L),
                    {TS, 0}
            end,
            State#state{pending=[], transit=Transit, waiting_for_ack=true, waiting_ack_tref=waiting_ack_timer(), bytes_send=NewBytesSend};
        {error, Reason} ->
            lager:warning("can't send ~p bytes to ~p due to ~p, reconnect!", [L, Node, Reason]),
            close_reconnect(State)
    end.

connect(#state{node=RemoteNode, reachable=false} = State) ->
    Self = self(),
    ConnectAsyncPid = spawn_link(fun() -> connect_async(Self, RemoteNode) end),
    State#state{async_connect_pid= ConnectAsyncPid}.

connect_async(ParentPid, RemoteNode) ->
    ConnectOpts = vmq_config:get_env(outgoing_connect_opts),
    % the outgoing_connect_params_module must implement the connect_params/1 function
    ConnectParamsMod = vmq_config:get_env(outgoing_connect_params_module),
    ConnectTimeout = vmq_config:get_env(outgoing_connect_timeout),
    Reply =
    case rpc:call(RemoteNode, ConnectParamsMod, connect_params, [node()]) of
        {Transport, Host, Port} ->
            case connect(Transport, Host, Port,
                         lists:usort([binary, {active, true},
                                      {keepalive, true},
                                      {send_timeout, 0}|ConnectOpts]), ConnectTimeout) of
                {ok, Socket} ->
                    % at least tune 'buffer'
                    {ok, BufSizes} = getopts(Socket, [sndbuf, recbuf, buffer]),
                    BufSize = lists:max([Sz || {_, Sz} <- BufSizes]),
                    setopts(Socket, [{buffer, BufSize}]),
                    case controlling_process(Transport, Socket, ParentPid) of
                        ok ->
                            {ok, {Transport, Socket}};
                        {error, Reason} ->
                            lager:debug("can't assign socket ownership to ~p due to ~p", [ParentPid, Reason]),
                            error
                    end;
                {error, Reason} ->
                    lager:warning("can't connect to cluster node ~p due to ~p", [RemoteNode, Reason]),
                    error
            end;
        {badrpc, nodedown} ->
            %% we don't scream.. vmq_cluster_mon screams
            error;
        E ->
            lager:warning("can't connect to cluster node ~p due to ~p", [RemoteNode, E]),
            error
    end,
    ParentPid ! {connect_async_done, self(), Reply}.

close_reconnect(#state{transport=Transport, socket=Socket} = State) ->
    close(Transport, Socket),
    State#state{async_connect_pid=undefined,
                reachable=false,
                socket=undefined,
                reconnect_tref=reconnect_timer()}.

reconnect_timer() ->
    erlang:send_after(?RECONNECT, self(), reconnect).

waiting_ack_timer() ->
    erlang:send_after(?ACK_TIMEOUT, self(), reconnect).

cancel_ack_timer(#state{waiting_ack_tref=undefined} = State) -> State;
cancel_ack_timer(#state{waiting_ack_tref=WaitingAckTref} = State) ->
    erlang:cancel_timer(WaitingAckTref),
    State#state{waiting_ack_tref=undefined}.

%% connect_params is called by a RPC
connect_params(_Node) ->
    case whereis(vmq_server_sup) of
        undefined ->
            %% vmq_server app not ready
            {error, not_ready};
        _ ->
            Listeners = vmq_config:get_env(listeners),
            MaybeSSLConfig = proplists:get_value(vmqs, Listeners, []),
            case connect_params(ssl, MaybeSSLConfig) of
                no_config ->
                    case proplists:get_value(vmq, Listeners) of
                        undefined ->
                            exit("can't connect to cluster node");
                        Config ->
                            connect_params(tcp, Config)
                    end;
                Config ->
                    Config
            end
    end.

getopts({ssl, Socket}, Opts) ->
    ssl:getopts(Socket, Opts);
getopts(Socket, Opts) ->
    inet:getopts(Socket, Opts).

setopts({ssl, Socket}, Opts) ->
    ssl:setopts(Socket, Opts);
setopts(Socket, Opts) ->
    inet:setopts(Socket, Opts).

connect_params(tcp, [{{Addr, Port}, _}|_]) ->
    {gen_tcp, Addr, Port};
connect_params(ssl, [{{Addr, Port}, _}|_]) ->
    {ssl, Addr, Port};
connect_params(_, []) -> no_config.

send(gen_tcp, Socket, Msg) ->
    gen_tcp:send(Socket, Msg);
send(ssl, Socket, Msg) ->
    ssl:send(Socket, Msg).

close(_, undefined) -> ok;
close(gen_tcp, Socket) ->
    gen_tcp:close(Socket);
close(ssl, Socket) ->
    ssl:close(Socket).

connect(gen_tcp, Host, Port, Opts, Timeout) ->
    gen_tcp:connect(Host, Port, Opts, Timeout);
connect(ssl, Host, Port, Opts, Timeout) ->
    ssl:connect(Host, Port, Opts, Timeout).

controlling_process(gen_tcp, Socket, Pid) ->
    gen_tcp:controlling_process(Socket, Pid);
controlling_process(ssl, {'ssl',Socket}, Pid) ->
    ssl:controlling_process(Socket, Pid).

teardown(#state{socket = Socket, transport = Transport, async_connect_pid = AsyncPid}, Reason) ->
    case AsyncPid of
        undefined -> ignore;
        Pid -> exit(Pid, normal)
    end,
    case Reason of
        normal ->
            lager:debug("normally stopped", []);
        shutdown ->
            lager:debug("stopped due to shutdown", []);
        _ ->
            lager:warning("stopped abnormally due to '~p'", [Reason])
    end,
    close(Transport, Socket),
    ok.

system_continue(_, _, State) ->
	loop(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, State) -> teardown(State, Reason).

system_code_change(Misc, _, _, _) ->
	{ok, Misc}.
