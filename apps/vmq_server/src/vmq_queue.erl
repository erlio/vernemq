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

-module(vmq_queue).
-include_lib("vmq_commons/include/vmq_types.hrl").
-include("vmq_server.hrl").

-behaviour(gen_fsm).

-ifdef(nowarn_gen_fsm).
-compile([{nowarn_deprecated_function,
          [{gen_fsm,start_link,3},
           {gen_fsm,reply,2},
           {gen_fsm,send_event,2},
           {gen_fsm,sync_send_event,3},
           {gen_fsm,send_all_state_event,2},
           {gen_fsm,sync_send_all_state_event,3},
           {gen_fsm,send_event_after,2},
           {gen_fsm,cancel_timer,1}]}]).
-endif.

%% API functions
-export([start_link/2,
         active/1,
         notify/1,
         notify_recv/1,
         enqueue/2,
         status/1,
         info/1,
         add_session/3,
         get_sessions/1,
         set_opts/2,
         get_opts/1,
         default_opts/0,
         set_last_waiting_acks/3,
         enqueue_many/2,
         enqueue_many/3,
         migrate/2,
         cleanup/2,
         force_disconnect/2,
         force_disconnect/3,
         set_delayed_will/3]).

-export([online/2, online/3,
         offline/2, offline/3,
         wait_for_offline/2, wait_for_offline/3,
         drain/2, drain/3]).

%% gen_server callbacks
-export([init/1,
         handle_sync_event/4,
         handle_event/3,
         handle_info/3,
         terminate/3,
         code_change/4]).


-record(queue, {
          queue = queue:new(),
          backup = queue:new(),
          type = fifo,
          max,
          size = 0,
          drop = 0
         }).

-record(session, {
          pid,
          cleanup_on_disconnect,
          status = notify,
          queue = #queue{},
          started_at :: vmq_time:timestamp(),
          queue_to_session_batch_size :: non_neg_integer()
         }).

-record(state, {
          id,
          offline = #queue{},
          deliver_mode = fanout,
          sessions = maps:new(),
          expiry_timer :: undefined | reference(),
          drain_time,
          drain_over_timer,
          drain_pending_batch :: undefined | {reference(), reference(), queue:new()},
          max_msgs_per_drain_step,
          waiting_call,
          opts,
          delayed_will :: {Delay :: non_neg_integer(),
                           Fun :: function()} | undefined,
          delayed_will_timer :: reference() | undefined,
          started_at :: vmq_time:timestamp(),
          initial_msg_id = 1 :: msg_id()
         }).

-type state() :: #state{}.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(SubscriberId :: subscriber_id(), Clean :: boolean()) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(SubscriberId, Clean) ->
    gen_fsm:start_link(?MODULE, [SubscriberId, Clean], []).


active(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {change_state, active, self()}).

notify(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {change_state, notify, self()}).

notify_recv(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {notify_recv, self()}).

enqueue(Queue, Msg) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {enqueue, to_internal(Msg)}).

enqueue_many(Queue, Msgs) when is_pid(Queue) and is_list(Msgs) ->
    NMsgs = lists:map(fun to_internal/1, Msgs),
    gen_fsm:sync_send_event(Queue, {enqueue_many, NMsgs}, infinity).

enqueue_many(Queue, Msgs, Opts) when is_pid(Queue), is_list(Msgs), is_map(Opts) ->
    NMsgs = lists:map(fun to_internal/1, Msgs),
    gen_fsm:sync_send_event(Queue, {enqueue_many, NMsgs, Opts}, infinity).

-spec add_session(pid(), pid(), map()) -> {ok, #{initial_msg_id := msg_id()}} |
                                          {error, any()}.
add_session(Queue, SessionPid, Opts) when is_pid(Queue) ->
    gen_fsm:sync_send_event(Queue, {add_session, SessionPid, Opts}, infinity).

get_sessions(Queue) when is_pid(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, get_sessions, infinity).

set_opts(Queue, Opts) when is_pid(Queue) ->
    gen_fsm:sync_send_event(Queue, {set_opts, self(), Opts}, infinity).

get_opts(Queue) when is_pid(Queue) ->
    save_sync_send_all_state_event(Queue, get_opts).

force_disconnect(Queue, Reason) when is_pid(Queue) ->
    force_disconnect(Queue, Reason, false).
force_disconnect(Queue, Reason, DoCleanup) when is_pid(Queue) and is_boolean(DoCleanup) ->
    gen_fsm:sync_send_all_state_event(Queue, {force_disconnect, Reason, DoCleanup}, infinity).

set_delayed_will(Queue, Fun, Delay) when is_pid(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, {set_delayed_will, Fun, Delay}, infinity).

set_last_waiting_acks(Queue, WAcks, NextMsgId) ->
    gen_fsm:sync_send_event(Queue, {set_last_waiting_acks, WAcks, NextMsgId}, infinity).

migrate(Queue, Queue) ->
    %% this scenario can happen, due to the eventual migration kickoff
    ok;
migrate(Queue, OtherQueue) ->
    %% Migrate Messages of 'Queue' to 'OtherQueue'
    %% -------------------------------------------
    save_sync_send_event(Queue, {migrate, OtherQueue}).

cleanup(Queue, Reason) ->
    save_sync_send_event(Queue, {cleanup, Reason}).

status(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, status, infinity).

info(Queue) ->
    save_sync_send_all_state_event(Queue, info).

save_sync_send_event(Queue, Event) ->
    case catch gen_fsm:sync_send_event(Queue, Event, infinity) of
        {'EXIT', {normal, _}} ->
            ok;
        {'EXIT', {noproc, _}} ->
            ok;
        {'EXIT', Reason} ->
            exit(Reason);
        Ret ->
            Ret
    end.

save_sync_send_all_state_event(Queue, Event) ->
    case catch gen_fsm:sync_send_all_state_event(Queue, Event, infinity) of
        {'EXIT', {normal, _}} ->
            {error, noproc};
        {'EXIT', {noproc, _}} ->
            {error, noproc};
        {'EXIT', Reason} ->
            exit(Reason);
        Ret ->
            Ret
    end.

default_opts() ->
    #{allow_multiple_sessions => vmq_config:get_env(allow_multiple_sessions),
      max_online_messages => vmq_config:get_env(max_online_messages),
      max_offline_messages => vmq_config:get_env(max_offline_messages),
      queue_deliver_mode => vmq_config:get_env(queue_deliver_mode),
      queue_type => vmq_config:get_env(queue_type),
      max_drain_time => vmq_config:get_env(max_drain_time),
      max_msgs_per_drain_step => vmq_config:get_env(max_msgs_per_drain_step),
      is_plugin => false}.

%%%===================================================================
%%% gen_fsm state callbacks
%%%===================================================================
online({change_state, NewSessionState, SessionPid}, State) ->
    {next_state, online, change_session_state(NewSessionState, SessionPid, State)};
online({notify_recv, SessionPid}, #state{id=SId, sessions=Sessions} = State) ->
    #session{queue=#queue{backup=Backup} = Queue} = Session = maps:get(SessionPid, Sessions),
    cleanup_queue(SId, Backup),
    NewSessions = maps:update(SessionPid,
                              Session#session{queue=Queue#queue{backup=queue:new()}},
                              Sessions),
    _ = vmq_metrics:incr_queue_out(queue:len(Backup)),
    {next_state, online, State#state{sessions=NewSessions}};
online({enqueue, Msg}, State) ->
    _ = vmq_metrics:incr_queue_in(),
    {next_state, online, insert(Msg, State)};
online(Event, State) ->
    lager:error("got unknown event in online state ~p", [Event]),
    {next_state, online, State}.

online({set_opts, SessionPid, Opts}, _From, #state{opts=OldOpts} = State) ->
    MergedOpts = maps:merge(OldOpts, Opts),
    NewState1 = set_general_opts(MergedOpts, State#state{opts=MergedOpts}),
    NewState2 = set_session_opts(SessionPid, MergedOpts, NewState1),
    {reply, ok, online, NewState2};
online({add_session, SessionPid, #{allow_multiple_sessions := true} = Opts}, _From, State0) ->
    %% allow multiple sessions per queue
    RetOpts = #{initial_msg_id => State0#state.initial_msg_id},
    State1 = unset_timers(add_session_(SessionPid, Opts, State0)),
    {reply, {ok, RetOpts}, online, State1};
online({add_session, SessionPid, #{allow_multiple_sessions := false} = Opts}, From, State)
  when State#state.waiting_call == undefined ->
    %% forbid multiple sessions per queue,
    %% we've to disconnect currently attached sessions
    %% and wait with the reply until all the sessions
    %% have been disconnected
    #state{id = SubscriberId} = State,
    lager:debug("client ~p disconnected due to multiple sessions not allowed", [SubscriberId]),
    disconnect_sessions(?SESSION_TAKEN_OVER, State),
    {next_state, state_change(add_session, online, wait_for_offline),
     State#state{waiting_call={add_session, SessionPid, Opts, From}}};
online({migrate, OtherQueue}, From, State)
  when State#state.waiting_call == undefined ->
    disconnect_sessions(?DISCONNECT_MIGRATION, State),
    {next_state, state_change(migrate, online, wait_for_offline),
     State#state{waiting_call={migrate, OtherQueue, From}}};
online({set_last_waiting_acks, WAcks, NextMsgId}, _From, State) ->
    {reply, ok, online, handle_waiting_acks_and_msgs(WAcks, NextMsgId, State)};
online({enqueue_many, Msgs}, _From, State) ->
    _ = vmq_metrics:incr_queue_in(length(Msgs)),
    {reply, ok, online, insert_many(Msgs, State)};
online({enqueue_many, Msgs, Opts}, _From, State) ->
    enqueue_many_(Msgs, online, Opts, State);
online({cleanup, Reason}, From, State)
  when State#state.waiting_call == undefined ->
    disconnect_sessions(Reason, State),
    {next_state, state_change(cleanup, online, wait_for_offline),
     State#state{waiting_call={{cleanup, Reason}, From}}};
online(Event, _From, State) ->
    lager:error("got unknown sync event in online state ~p", [Event]),
    {reply, {error, online}, State}.

wait_for_offline({enqueue, Msg}, State) ->
    _ = vmq_metrics:incr_queue_in(),
    {next_state, wait_for_offline, insert(Msg, State)};
wait_for_offline(Event, State) ->
    lager:error("got unknown event in wait_for_offline state ~p", [Event]),
    {next_state, wait_for_offline, State}.


wait_for_offline({set_last_waiting_acks, WAcks, NextMsgId}, _From, State) ->
    {reply, ok, wait_for_offline, handle_waiting_acks_and_msgs(WAcks, NextMsgId, State)};
wait_for_offline({add_session, SessionPid, Opts}, From,
                 #state{waiting_call={migrate, _OtherQueue, MigrationFrom}} = State) ->
    %% Reason for this case:
    %% ---------------------
    %% This case handles a race condition that can happen when multiple
    %% concurrent clients (with clean_session=false) try to connect using
    %% the same client_id at the same time. The connect itself isn't necessarily
    %% the problem, as CONNECTs are synchronized using vmq_reg_sync. However
    %% faulty or dumb clients (with clean_session=false) that send a SUBSCRIBE
    %% every time after a CONNECT/CONNACK (e.g. not properly using the SessionPresent
    %% flag in returned CONNACK) may interfere with waiting migrate or add_session
    %% calls. The reason for this is that SUBSCRIBEs aren't synchronized while
    %% CONNECTs are.
    %%
    %% Precondition:
    %% -------------
    %% The calling session uses allow_register_during_netsplit=false for the session setup.
    %% The racing client are using clean_session=false
    %%
    %% Solution:
    %% ---------
    %% The waiting migration call isn't required anymore as we have a new session
    %% that can use this queue (and it's possible offline messages). As we haven't
    %% started to drain this queue (we are not in drain state) we can reply 'ok'
    %% to the waiting migration. The 'OtherQueue' that was part of the waiting
    %% migration gets eventually stopped.
    gen_fsm:reply(MigrationFrom, ok),
    {next_state, wait_for_offline,
     State#state{waiting_call={add_session, SessionPid, Opts, From}}};
wait_for_offline({add_session, NewSessionPid, NewOpts}, From,
                #state{waiting_call={add_session, SessionPid, _Opts, AddFrom}} = State) ->
    %% Reason for this case:
    %% ---------------------
    %% See case above!
    %%
    %% Precondition:
    %% -------------
    %% See case above!
    %%
    %% Solution:
    %% ---------
    %% The waiting add_session call isn't required anymore as we have a new session
    %% that we should attach to this queue. We can terminate the waiting add_session
    %% and replace the waiting_call
    Opts = #{initial_msg_id => State#state.initial_msg_id},
    gen_fsm:reply(AddFrom, {ok, Opts}),
    exit(SessionPid, normal),
    {next_state, wait_for_offline,
     State#state{waiting_call={add_session, NewSessionPid, NewOpts, From}}};
wait_for_offline({add_session, _NewSessionPid, _NewOpts}, _From,
                #state{waiting_call={{cleanup, Reason}, _CleanupFrom}} = State) ->
    %% Reason for this case:
    %% ---------------------
    %% a synchronized registration that had triggered a cleanup got
    %% superseeded by a non-synchronized registration (e.g. allow_multiple_sessions=true)
    %%
    %% Solution:
    %% ---------
    %% We forcefully terminate the add_session call, as we have to ensure that
    %% this queue is completely wiped, before we allow new sessions to join.
    {reply, {error, {cleanup, Reason}}, wait_for_offline, State};
wait_for_offline(Event, _From, State) ->
    lager:error("got unknown sync event in wait_for_offline state ~p", [Event]),
    {reply, {error, wait_for_offline}, wait_for_offline, State}.

drain(drain_start, #state{id=SId, offline=#queue{queue=Q} = Queue,
                          drain_time=DrainTimeout,
                          max_msgs_per_drain_step=DrainStepSize,
                          waiting_call={migrate, RemoteQueue, _From}} = State) ->
    {DrainQ, NewQ} = queue_split(DrainStepSize, Q),
    #queue{queue=DecompressedDrainQ} = decompress_queue(SId, #queue{queue=DrainQ}),
    case queue:to_list(DecompressedDrainQ) of
        [] ->
            %% no need to drain!

            %% the extra timeout gives the chance that pending messages
            %% in the erlang mailbox could still get enqueued and
            %% therefore eventually transmitted to the remote queue
            {next_state, drain,
             State#state{drain_over_timer=gen_fsm:send_event_after(DrainTimeout, drain_over),
                         offline=Queue#queue{size=0, drop=0, queue=queue:new()}}};
        Msgs ->
            %% remote_enqueue triggers an enqueue_many inside the remote queue
            %% but forces the traffic to go over the distinct communication link
            %% instead of the erlang distribution link.
            ExtMsgs = lists:map(fun to_external/1, Msgs),
            {MRef, Ref} = vmq_cluster:remote_enqueue_async(node(RemoteQueue), {enqueue, RemoteQueue, ExtMsgs}, false),
            {next_state, drain,
             State#state{offline=Queue#queue{size=queue:len(NewQ), drop=0, queue=NewQ},
                         drain_pending_batch={MRef, Ref, DrainQ}}}
    end;
drain({enqueue, Msg}, #state{drain_over_timer=TRef} =  State) ->
    %% even in drain state it is possible that an enqueue message
    %% reaches this process, so we've to queue this message otherwise
    %% it would be lost.
    gen_fsm:cancel_timer(TRef),
    gen_fsm:send_event(self(), drain_start),
    _ = vmq_metrics:incr_queue_in(),
    {next_state, drain, insert(Msg, State)};

drain(drain_over, #state{waiting_call={migrate, _, From}} =
      #state{offline=#queue{size=0}} = State) ->
    %% we're done with the migrate, offline queue is empty
    gen_fsm:reply(From, ok),
    {stop, normal, State};
drain(drain_over, State) ->
    %% we still have undrained messages in the offline queue
    gen_fsm:send_event(self(), drain_start),
    {next_state, drain, State};
drain(Event, State) ->
    lager:error("got unknown event in drain state ~p", [Event]),
    {next_state, drain, State}.

drain({enqueue_many, Msgs}, _From, #state{drain_over_timer=TRef} =  State) ->
    gen_fsm:cancel_timer(TRef),
    gen_fsm:send_event(self(), drain_start),
    _ = vmq_metrics:incr_queue_in(length(Msgs)),
    {reply, ok, drain, insert_many(Msgs, State)};
drain({enqueue_many, Msgs, Opts}, _From, #state{drain_over_timer=TRef} =  State) ->
    gen_fsm:cancel_timer(TRef),
    gen_fsm:send_event(self(), drain_start),
    enqueue_many_(Msgs, drain, Opts, State);
drain(Event, _From, State) ->
    lager:error("got unknown sync event in drain state ~p", [Event]),
    {reply, {error, draining}, drain, State}.


offline(init_offline_queue, #state{id=SId} = State) ->
    case vmq_message_store:find(SId, queue_init) of
        {ok, MsgRefs} ->
            _ = vmq_metrics:incr_queue_initialized_from_storage(),
            {next_state, offline,
             insert_many(MsgRefs, maybe_set_expiry_timer(State))};
        {error, no_matching_hook_found} ->
            % that's ok
            _ = vmq_metrics:incr_queue_initialized_from_storage(),
            {next_state, offline, maybe_set_expiry_timer(State)};
        {error, Reason} ->
            lager:error("can't initialize queue from offline storage due to ~p, retry in 1 sec", [Reason]),
            gen_fsm:send_event_after(1000, init_offline_queue),
            {next_state, offline, State}
    end;
offline({enqueue, Enq}, #state{id=SId} = State) ->
    case Enq of
        #deliver{qos=QoS, msg=#vmq_msg{routing_key=Topic,
                                       payload=Payload,
                                       retain=Retain}} ->
            _ = vmq_plugin:all(on_offline_message, [SId, QoS, Topic, Payload, Retain]);
        _ ->
            ignore
    end,
    %% storing the message in the offline queue
    _ = vmq_metrics:incr_queue_in(),
    {next_state, offline, insert(Enq, State)};
offline(expire_session, #state{id=SId, offline=#queue{queue=Q}} = State) ->
    %% session has expired cleanup and go down
    UnsubbedTopics = get_topics(SId),
    vmq_plugin:all(on_topic_unsubscribed, [SId, UnsubbedTopics]),
    vmq_reg:delete_subscriptions(SId),
    cleanup_queue(SId, Q),
    _ = vmq_plugin:all(on_session_expired, [SId]),
    _ = vmq_metrics:incr_queue_unhandled(queue:len(Q)),
    State1 = publish_last_will(State),
    {stop, normal, State1};
offline(publish_last_will, State) ->
    State1 = unset_will_timer(publish_last_will(State)),
    {next_state, offline, State1};
offline(Event, State) ->
    lager:error("got unknown event in offline state ~p", [Event]),
    {next_state, offline, State}.
offline({add_session, SessionPid, Opts}, _From, State) ->
    ReturnOpts = #{initial_msg_id => State#state.initial_msg_id},
    {reply, {ok, ReturnOpts}, state_change(add_session, offline, online),
     unset_timers(add_session_(SessionPid, Opts, State))};
offline({migrate, OtherQueue}, From, State) ->
    gen_fsm:send_event(self(), drain_start),
    {next_state, state_change(migrate, offline, drain),
     State#state{waiting_call={migrate, OtherQueue, From}}};
offline({enqueue_many, Msgs}, _From, State) ->
    _ = vmq_metrics:incr_queue_in(length(Msgs)),
    {reply, ok, offline, insert_many(Msgs, State)};
offline({enqueue_many, Msgs, Opts}, _From, State) ->
    enqueue_many_(Msgs, offline, Opts, State);
offline({cleanup, _Reason}, _From, #state{id=SId, offline=#queue{queue=Q}} = State) ->
    cleanup_queue(SId, Q),
    _ = vmq_metrics:incr_queue_unhandled(queue:len(Q)),
    {stop, normal, ok, State};
offline(Event, _From, State) ->
    lager:error("got unknown sync event in offline state ~p", [Event]),
    {reply, {error, offline}, offline, State}.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================


init([SubscriberId, Clean]) ->
    Defaults = default_opts(),
    #{max_offline_messages := MaxOfflineMsgs,
      queue_deliver_mode := DeliverMode,
      queue_type := QueueType,
      max_drain_time := DrainTime,
      max_msgs_per_drain_step := MaxMsgsPerDrainStep} = Defaults,
    OfflineQueue = #queue{type=QueueType, max=MaxOfflineMsgs},
    rand:seed(exsplus, os:timestamp()),
    case Clean of
        true ->
            ignore;
        false ->
            %% only init offline queue if this is a queue generated
            %% during broker startup or we were restarted by the
            %% vmq_queue_sup supervisor
            gen_fsm:send_event(self(), init_offline_queue)
    end,
    vmq_metrics:incr_queue_setup(),
    {ok, offline,  #state{id=SubscriberId,
                          offline=OfflineQueue,
                          drain_time=DrainTime,
                          deliver_mode=DeliverMode,
                          max_msgs_per_drain_step=MaxMsgsPerDrainStep,
                          opts=Defaults,
                          started_at=vmq_time:timestamp(millisecond)
                         }}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName,
                  #state{deliver_mode=Mode, offline=#queue{size=OfflineSize},
                         sessions=Sessions, opts=#{is_plugin := IsPlugin}} = State) ->
    TotalStoredMsgs =
    maps:fold(fun(_, #session{queue=#queue{size=Size}}, Acc) ->
                      Acc + Size
              end, OfflineSize, Sessions),
    {reply, {StateName, Mode, TotalStoredMsgs, maps:size(Sessions), IsPlugin}, StateName, State};
handle_sync_event(info, _From, StateName,
                  #state{deliver_mode=Mode,
                         offline=#queue{size=OfflineSize},
                         sessions=Sessions,
                         opts=#{is_plugin := IsPlugin},
                         started_at=StartedAt} = State) ->
    {OnlineMessages, SessionInfo} =
    maps:fold(fun(_, #session{pid=SessPid, cleanup_on_disconnect=Clean,
                              queue=#queue{size=Size},
                              started_at=SessionStartedAt}, {AccN, AccSess}) ->
                      {AccN + Size, [{SessPid, Clean,SessionStartedAt}|AccSess]}
              end, {0, []}, Sessions),
    Info = #{is_offline => (StateName == offline),
             is_online => (StateName /= offline),
             statename => StateName,
             deliver_mode => Mode,
             offline_messages => OfflineSize,
             online_messages => OnlineMessages,
             num_sessions => length(SessionInfo),
             is_plugin => IsPlugin,
             sessions => SessionInfo,
             started_at => StartedAt},
    {reply, Info, StateName, State};

handle_sync_event(get_sessions, _From, StateName, #state{sessions=Sessions} = State) ->
    {reply, maps:keys(Sessions), StateName, State};
handle_sync_event(get_opts, _From, StateName, #state{opts=Opts} = State) ->
    {reply, Opts, StateName, State};
handle_sync_event({force_disconnect, Reason, DoCleanup}, _From, StateName,
                  #state{id=SId, sessions=Sessions, offline=#queue{queue=OfflineQ}} = State) ->
    %% Forcefully disconnect all sessions and cleanup all state
    case DoCleanup of
        true ->
            UnsubbedTopics = get_topics(SId),
            vmq_plugin:all(on_topic_unsubscribed, [SId, UnsubbedTopics]),
            vmq_reg:delete_subscriptions(SId),
            %% Collect all queues, make sure to include the backups
            SessionQueues = [queue:join(BQ, Q) || #session{queue=#queue{queue=Q, backup=BQ}} <- maps:values(Sessions)],
            lists:foreach(
              fun(Q) ->
                      cleanup_queue(SId, Q),
                      _ = vmq_metrics:incr_queue_unhandled(queue:len(Q))
              end, [OfflineQ | SessionQueues]),
            {stop, normal, ok, State};
        false ->
            disconnect_sessions(Reason, State),
            {reply, ok, StateName, State}
    end;
handle_sync_event({set_delayed_will, Fun, Delay}, _From, StateName, State) ->
    {reply, ok, StateName, State#state{delayed_will = {Delay, Fun}}};
handle_sync_event(Event, _From, _StateName, State) ->
    {stop, {error, {unknown_sync_event, Event}}, State}.

handle_info({Ref, ok}, drain,
            #state{id=SId,
                   drain_pending_batch = {MRef, Ref, Batch}} = State) ->
    %% remote async enqueue completed successfully.
    demonitor(MRef, [flush]),
    cleanup_queue(SId, Batch),
    _ = vmq_metrics:incr_queue_out(queue:len(Batch)),
    gen_fsm:send_event(self(), drain_start),
    {next_state, drain,
     State#state{drain_pending_batch=undefined}};
handle_info({Ref, {error, not_reachable}}, drain,
            #state{drain_pending_batch={MRef, Ref, Batch},
                   drain_time=DrainTimeout,
                   offline=#queue{queue=Q}} = State) ->
    %% remote async enqueue failed, let's retry
    demonitor(MRef, [flush]),
    gen_fsm:send_event_after(DrainTimeout, drain_start),
    {next_state, drain,
     State#state{drain_pending_batch=undefined,
                 offline=#queue{queue=queue:join(Batch, Q)}}};
handle_info({Ref, {error, Reason}}, drain,
            #state{id=SId, drain_pending_batch={MRef, Ref, Batch},
                   offline=#queue{queue=Q},
                   waiting_call={migrate, RemoteQueue, From}} = State) ->
    demonitor(MRef, [flush]),
    %% remote async enqueue failed, for another reason.  this
    %% shouldn't happen, as the register_subscriber is synchronized
    %% using the vmq_reg_leader process. However this could
    %% theoretically happen in case of an inconsistent (but
    %% un-detected) cluster state.  we don't drain in this case.
    lager:error("can't drain queue '~p' for [~p][~p] due to ~p",
                [SId, self(), RemoteQueue, Reason]),
    gen_fsm:reply(From, ok),
    %% transition to offline, and let a future session drain this queue
    {next_state, state_change(drain_error, drain, offline),
     State#state{waiting_call=undefined,
                 drain_pending_batch=undefined,
                 offline=#queue{queue=queue:join(Batch, Q)}}};
handle_info({'DOWN', MRef, process, _, Reason}, drain,
            #state{id=SId, drain_pending_batch={MRef, _Ref, Batch},
                   drain_time=DrainTimeout,
                   offline=#queue{queue=Q},
                   waiting_call={migrate, RemoteQueue, _From}} = State) ->
    lager:warning("drain queue '~p' for [~p][~p] remote_enqueue failed due to ~p",
                  [SId, self(), RemoteQueue, Reason]),
    gen_fsm:send_event_after(DrainTimeout, drain_start),
    {next_state, drain,
     State#state{drain_pending_batch=undefined,
                 offline=#queue{queue=queue:join(Batch, Q)}}};
handle_info({'DOWN', _MRef, process, Pid, _}, StateName, State) ->
    handle_session_down(Pid, StateName, State);
handle_info(Info, StateName, State) ->
    lager:error("got unknown handle_info in ~p state ~p", [StateName, Info]),
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    vmq_metrics:incr_queue_teardown(),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
enqueue_many_(Msgs, FsmState, #{states := AllowedStates}, State) ->
    case allowed_state(FsmState, AllowedStates) of
        true ->
            _ = vmq_metrics:incr_queue_in(length(Msgs)),
            {reply, ok, FsmState, insert_many(Msgs, State)};
        false ->
            {reply, {error, FsmState}, FsmState, State}
    end.

allowed_state(FsmState, AllowedStates) ->
    Any = lists:member(any, AllowedStates),
    Any orelse lists:member(FsmState, AllowedStates).

add_session_(SessionPid, Opts, #state{id=SId, offline=Offline,
                                      sessions=Sessions, opts=OldOpts} = State) ->
    #{max_online_messages := MaxOnlineMessages,
      max_offline_messages := MaxOfflineMsgs,
      queue_deliver_mode := DeliverMode,
      queue_type := QueueType,
      cleanup_on_disconnect := Clean
     } = Opts,
    BatchSize=maps:get(queue_to_session_batch_size, Opts, 100),
    NewSessions =
    case maps:get(SessionPid, Sessions, not_found) of
        not_found ->
            _ = vmq_plugin:all(on_client_wakeup, [SId]),
            monitor(process, SessionPid),
            maps:put(SessionPid,
                     #session{pid=SessionPid, cleanup_on_disconnect=Clean,
                              queue=#queue{max=MaxOnlineMessages},
                              started_at=vmq_time:timestamp(millisecond),
                              queue_to_session_batch_size=BatchSize}, Sessions);
        _ ->
            Sessions
    end,
    insert_from_queue(Offline#queue{type=QueueType},
                      State#state{
                        deliver_mode=DeliverMode,
                        offline=Offline#queue{
                                  max=MaxOfflineMsgs,
                                  type=QueueType,
                                  size=0, drop=0,
                                  queue = queue:new()
                                 },
                        opts=maps:merge(OldOpts, Opts),
                        sessions=NewSessions}).

del_session(SessionPid, #state{id=SId, sessions=Sessions} = State) ->
    NewSessions = maps:remove(SessionPid, Sessions),
    case maps:get(SessionPid, Sessions) of
        #session{cleanup_on_disconnect=true} = Session ->
            cleanup_session(SId, Session),
            {State#state{sessions=NewSessions}, Session};
        Session ->
            %% give queue content of this session to other alive sessions
            %% or to offline queue
            {insert_from_session(Session, State#state{sessions=NewSessions}),
             Session}
    end.

handle_session_down(SessionPid, StateName,
                    #state{id=SId, waiting_call=WaitingCall} = State) ->
    {NewState, DeletedSession} = del_session(SessionPid, State),
    case {maps:size(NewState#state.sessions), StateName, WaitingCall} of
        {0, wait_for_offline, {add_session, NewSessionPid, Opts, From}} ->
            %% last session gone
            %% ... but we've a new session waiting
            %%     no need to go into offline state
            RetOpts = #{initial_msg_id => State#state.initial_msg_id},
            gen_fsm:reply(From, {ok, RetOpts}),
            case DeletedSession#session.cleanup_on_disconnect of
                true ->
                    _ = vmq_plugin:all(on_client_gone, [SId]);
                false ->
                    _ = vmq_plugin:all(on_client_offline, [SId])
            end,
            {next_state, state_change({'DOWN', add_session}, wait_for_offline, online),
             add_session_(NewSessionPid, Opts, NewState#state{waiting_call=undefined})};
        {0, wait_for_offline, {migrate, _, From}} when DeletedSession#session.cleanup_on_disconnect ->
            %% last session gone
            %% ... we dont need to migrate this one
            UnsubbedTopics = get_topics(SId),
            vmq_plugin:all(on_topic_unsubscribed, [SId, UnsubbedTopics]),
            vmq_reg:delete_subscriptions(SId),
            _ = vmq_plugin:all(on_client_gone, [SId]),
            gen_fsm:reply(From, ok),
            {stop, normal, NewState};
        {0, wait_for_offline, {migrate, _, _}} ->
            %% last session gone
            %% ... but we've a migrate request waiting
            %%     go into drain state
            gen_fsm:send_event(self(), drain_start),
            _ = vmq_plugin:all(on_client_offline, [SId]),
            {next_state, state_change({'DOWN', migrate}, wait_for_offline, drain), NewState};
        {0, wait_for_offline, {{cleanup, _Reason}, From}} ->
            %% Forcefully cleaned up, we have to cleanup remaining offline messages
            %% we don't cleanup subscriptions!
            #state{offline=#queue{queue=Q}} = NewState,
            cleanup_queue(SId, Q),
            _ = vmq_metrics:incr_queue_unhandled(queue:len(Q)),
            gen_fsm:reply(From, ok),
            _ = vmq_plugin:all(on_client_gone, [SId]),
            {stop, normal, NewState};
        {0, _, _} when DeletedSession#session.cleanup_on_disconnect ->
            %% last session gone
            %% ... we've to cleanup and go down
            %%
            %% it is assumed that all attached sessions use the same
            %% clean session flag
            UnsubbedTopics = get_topics(SId),
            vmq_plugin:all(on_topic_unsubscribed, [SId, UnsubbedTopics]),
            vmq_reg:delete_subscriptions(SId),
            _ = vmq_plugin:all(on_client_gone, [SId]),
            {stop, normal, NewState};
        {0, OldStateName, _} ->
            %% last session gone
            %% ... we've to stay around and store the messages
            %%     inside the offline queue
            _ = vmq_plugin:all(on_client_offline, [SId]),
            {next_state, state_change('DOWN', OldStateName, offline),
             maybe_set_last_will_timer(maybe_set_expiry_timer(NewState#state{offline=compress_queue(SId, NewState#state.offline)}))};
        _ ->
            %% still one or more sessions online
            {next_state, StateName, NewState}
    end.

handle_waiting_acks_and_msgs(WAcks, NextMsgId, #state{id=SId, sessions=Sessions, offline=Offline} = State) ->
    %% we can only handle the last waiting acks and msgs if this is
    %% the last session active for this queue.
    case maps:size(Sessions) of
        1 ->
            %% this is the last active session
            NewOfflineQueue =
            lists:foldl(
              fun(#deliver{msg=#vmq_msg{persisted=true} = Msg}=D, AccOffline) ->
                      queue_insert(true, D#deliver{msg=Msg#vmq_msg{persisted=false}}, AccOffline, SId);
                 (Msg, AccOffline) ->
                      queue_insert(true, Msg, AccOffline, SId)
              end, Offline, WAcks),
            State#state{offline=NewOfflineQueue, initial_msg_id=NextMsgId};
        N ->
            lager:debug("handle waiting acks for multiple sessions (~p) not possible", [N]),
            %% it doesn't make sense to keep the waiting acks around
            %% however depending on the balancing strategy it would
            %% make sense to re-enqueue messages for other active
            %% sessions --> TODO
            State
    end.

disconnect_sessions(Reason, #state{sessions=Sessions}) ->
    maps:fold(fun(SessionPid, #session{}, _) ->
                      %% before the session is going to die it
                      %% will send out LWT messages and will give
                      %% us back the waiting acks and messages
                      %% calling set_last_waiting_acks/2
                      %% then the 'DOWN' message gets triggerd
                      %% finally deleting the session
                      vmq_mqtt_fsm_util:send(SessionPid, {disconnect, Reason}),
                      ok
              end, ok, Sessions).

change_session_state(NewState, SessionPid, #state{id=SId, sessions=Sessions} = State) ->
    #session{queue=#queue{backup=Backup} = Queue} = Session = maps:get(SessionPid, Sessions),
    cleanup_queue(SId, Backup),
    _ = vmq_metrics:incr_queue_out(queue:len(Backup)),
    UpdatedSession = change_session_state_(NewState, SId,
                                           Session#session{queue=Queue#queue{backup=queue:new()}}),
    NewSessions = maps:update(SessionPid, UpdatedSession, Sessions),
    State#state{sessions=NewSessions}.

%% in active state
change_session_state_(active, _SId, #session{status=active} = Session) ->
    Session;
change_session_state_(notify, _SId, #session{status=active} = Session) ->
    Session#session{status=notify};

%% in passive state
change_session_state_(notify, _SId, #session{status=passive, queue=#queue{size=0}} = Session) ->
    Session#session{status=notify};
change_session_state_(notify, _SId, #session{status=passive} = Session) ->
    send_notification(Session);
change_session_state_(active, _SId, #session{status=passive, queue=#queue{size=0}} = Session) ->
    Session#session{status=active};
change_session_state_(active, SId, #session{status=passive} = Session) ->
    send(SId, Session);

%% in notify state
change_session_state_(active, _SId, #session{status=notify, queue=#queue{size=0}} = Session) ->
    Session#session{status=active};
change_session_state_(active, SId, #session{status=notify} = Session) ->
    send(SId, Session);
change_session_state_(notify, _SId, #session{status=notify} = Session) ->
    Session.

insert_from_session(#session{queue=Queue},
                    #state{deliver_mode=fanout, sessions=Sessions} = State)
  when Sessions == #{} ->
    %% all will go into offline queue
    insert_from_queue(Queue, State);
insert_from_session(_, #state{deliver_mode=fanout} = State) ->
    %% due to fanout other sessions have already received the messages
    State;
insert_from_session(#session{queue=Queue},
                    #state{deliver_mode=balance} = State) ->
    %% allow other sessions to balance the messages of the dead queue
    insert_from_queue(Queue, State).

insert_from_queue(#queue{type=fifo, queue=Q, backup=BQ}, State) ->
    insert_from_queue(fun queue:out/1, queue:out(queue:join(BQ, Q)), State);
insert_from_queue(#queue{type=lifo, queue=Q, backup=BQ}, State) ->
    insert_from_queue(fun queue:out_r/1, queue:out_r(queue:join(BQ, Q)), State).

insert_from_queue(F, {{value, Msg}, Q}, State) when is_tuple(Msg) ->
    insert_from_queue(F, F(Q), insert(Msg, State));
insert_from_queue(F, {{value, MsgRef}, Q}, State) when is_binary(MsgRef) ->
    insert_from_queue(F, F(Q), insert(MsgRef, State));
insert_from_queue(_F, {empty, _}, State) ->
    State.

insert_many(MsgsOrRefs, State) ->
    lists:foldl(fun(MsgOrRef, AccState) ->
                        insert(MsgOrRef, AccState)
                end, State, MsgsOrRefs).

%% Offline Queue
-spec insert(deliver() | msg_ref(), state()) -> state().
insert(#deliver{qos=0}, #state{sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, skip message for QoS0 Subscription
    State;
insert(#deliver{msg=#vmq_msg{qos=0}}, #state{sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, skip QoS0 message for QoS1 or QoS2 Subscription
    State;
insert(MsgOrRef, #state{id=SId, offline=Offline, sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, insert in offline queue
    State#state{offline=queue_insert(true, maybe_set_expiry_ts(MsgOrRef), Offline, SId)};

%% Online Queue
insert(MsgOrRef, #state{id=SId, deliver_mode=fanout, sessions=Sessions} = State) ->
    {NewSessions, _} = session_fold(SId, fun session_insert/3, maybe_set_expiry_ts(MsgOrRef), Sessions),
    State#state{sessions=NewSessions};

insert(MsgOrRef, #state{id=SId, deliver_mode=balance, sessions=Sessions} = State) ->
    Pids = maps:keys(Sessions),
    RandomPid = lists:nth(rand:uniform(maps:size(Sessions)), Pids),
    RandomSession = maps:get(RandomPid, Sessions),
    {UpdatedSession, _} = session_insert(SId, RandomSession, maybe_set_expiry_ts(MsgOrRef)),
    State#state{sessions=maps:update(RandomPid, UpdatedSession, Sessions)}.

session_insert(SId, #session{status=active, queue=Q} = Session, MsgOrRef) ->
    {send(SId, Session#session{queue=queue_insert(false, MsgOrRef, Q, SId)}), MsgOrRef};
session_insert(SId, #session{status=passive, queue=Q} = Session, MsgOrRef) ->
    {Session#session{queue=queue_insert(false, MsgOrRef, Q, SId)}, MsgOrRef};
session_insert(SId, #session{status=notify, queue=Q} = Session, MsgOrRef) ->
    {send_notification(Session#session{queue=queue_insert(false, MsgOrRef, Q, SId)}), MsgOrRef}.

%% unlimited messages accepted
queue_insert(Offline, MsgOrRef, #queue{max=-1, size=Size, queue=Queue} = Q, SId) ->
    Q#queue{queue=queue:in(maybe_offline_store(Offline, SId, MsgOrRef), Queue), size=Size + 1};
%% tail drop in case of fifo
queue_insert(_Offline, MsgOrRef, #queue{type=fifo, max=Max, size=Size, drop=Drop} = Q, SId)
  when Size >= Max ->
    on_message_drop_hook(SId, MsgOrRef, queue_full),
    vmq_metrics:incr_queue_drop(),
    maybe_offline_delete(SId, MsgOrRef),
    Q#queue{drop=Drop + 1};
%% drop oldest in case of lifo
queue_insert(Offline, MsgOrRef, #queue{type=lifo, max=Max, size=Size, queue=Queue, drop=Drop} = Q, SId)
  when Size >= Max ->
    {{value, OldMsgOrRef}, NewQueue} = queue:out(Queue),
    on_message_drop_hook(SId, OldMsgOrRef, queue_full),
    vmq_metrics:incr_queue_drop(),
    maybe_offline_delete(SId, OldMsgOrRef),
    Q#queue{queue=queue:in(maybe_offline_store(Offline, SId, MsgOrRef), NewQueue), drop=Drop + 1};

%% normal enqueue
queue_insert(Offline, MsgOrRef, #queue{queue=Queue, size=Size} = Q, SId) ->
    Q#queue{queue=queue:in(maybe_offline_store(Offline, SId, MsgOrRef), Queue), size=Size + 1}.

send(SId, #session{pid=Pid, queue=Q,
                  queue_to_session_batch_size = BatchSize} = Session) ->
    Session#session{status=passive, queue=send(SId, Pid, BatchSize, Q)}.

send(SId, Pid, BatchSize, #queue{type=fifo, queue=Queue, size=Count, drop=Dropped} = Q) ->
    {Batch, NewQueue, NewCount} = prepare_msgs(SId, queue:new(), Queue, Count, BatchSize),
    Msgs = queue:to_list(Batch),
    vmq_mqtt_fsm_util:send(Pid, {mail, self(), Msgs, Count, Dropped}),
    Q#queue{queue=NewQueue, backup=Batch, size=NewCount, drop=0};
send(SId, Pid, _BatchSize, #queue{type=lifo, queue=Queue, size=Count, drop=Dropped} = Q) ->
    Msgs = maybe_expire_msgs(SId, lists:reverse(queue:to_list(Queue))),
    vmq_mqtt_fsm_util:send(Pid, {mail, self(), Msgs, Count, Dropped}),
    Q#queue{queue=queue:new(), backup=Queue, size=0, drop=0}.

prepare_msgs(_SId, OQ, Q, QC, 0) ->
    {OQ, Q, QC};
prepare_msgs(SId, OQ, Q, QC, N) ->
    case queue:out(Q) of
        {{value, Msg}, NQ} ->
            case maybe_deref(SId, Msg) of
                {ok, NewMsg} ->
                    case maybe_expire_msg(SId, NewMsg) of
                        expired ->
                            prepare_msgs(SId, OQ, NQ, QC - 1, N);
                        NewMsg1 ->
                            NOQ = queue:in(NewMsg1, OQ),
                            prepare_msgs(SId, NOQ, NQ, QC - 1, N-1)
                    end;
                {error, _} ->
                    prepare_msgs(SId, OQ, NQ, QC - 1, N)
            end;
        {empty, _} ->
            {OQ, Q, QC}
    end.

maybe_deref(SId, MsgRef) when is_binary(MsgRef) ->
    case vmq_message_store:read(SId, MsgRef) of
        {ok, #vmq_msg{qos=QoS}=Msg} ->
            {ok, #deliver{qos=QoS, msg=Msg}};
        {error, _} = E ->
            E
    end;
maybe_deref(_, Msg) ->
    {ok, Msg}.


send_notification(#session{pid=Pid} = Session) ->
    vmq_mqtt_fsm_util:send(Pid, {mail, self(), new_data}),
    Session#session{status=passive}.

cleanup_session(SubscriberId, #session{queue=#queue{queue=Q, backup=BQ}}) ->
    _ = vmq_metrics:incr_queue_unhandled(queue:len(Q)),
    %% it's possible that the backup queue isn't cleaned up yet.
    cleanup_queue(SubscriberId, queue:join(Q, BQ)).

cleanup_queue(_, {[],[]}) -> ok; %% optimization
cleanup_queue(SId, Queue) ->
    cleanup_queue_(SId, queue:out(Queue)).

cleanup_queue_(SId, {{value, #deliver{} = Msg}, NewQueue}) ->
    maybe_offline_delete(SId, Msg),
    cleanup_queue_(SId, queue:out(NewQueue));
cleanup_queue_(SId, {{value, {deliver_pubrel, _}}, NewQueue}) ->
    % no need to deref
    cleanup_queue_(SId, queue:out(NewQueue));
cleanup_queue_(SId, {{value, MsgRef}, NewQueue}) when is_binary(MsgRef) ->
    maybe_offline_delete(SId, MsgRef),
    cleanup_queue_(SId, queue:out(NewQueue));
cleanup_queue_(_, {empty, _}) -> ok.


session_fold(SId, Fun, Acc, Map) ->
    session_fold(SId, Fun, Acc, Map, maps:keys(Map)).

session_fold(SId, Fun, Acc, Map, [K|Rest]) ->
    {NewV, NewAcc} = Fun(SId, maps:get(K, Map), Acc),
    session_fold(SId, Fun, NewAcc, maps:update(K, NewV, Map), Rest);
session_fold(_, _, Acc, Map, []) ->
    {Map, Acc}.

maybe_set_expiry_timer(#state{sessions=Sessions,
                              opts=Opts}=State) when Sessions == #{} ->
    Duration =
        case Opts of
            #{session_expiry_interval := ExpiryInterval} ->
                ExpiryInterval;
            _ ->
                vmq_config:get_env(persistent_client_expiration, 0)
        end,
    maybe_set_expiry_timer(Duration, State);
maybe_set_expiry_timer(State) -> State.

maybe_set_expiry_timer(0, State) ->
    %% never expire
    State;
maybe_set_expiry_timer(ExpireAfter, State) when ExpireAfter > 0 ->
    Ref = gen_fsm:send_event_after(ExpireAfter * 1000, expire_session),
    State#state{expiry_timer=Ref}.

maybe_set_last_will_timer(#state{delayed_will = undefined} = State) ->
    State;
maybe_set_last_will_timer(#state{delayed_will = {Delay, _Fun}} = State) ->
    Ref = gen_fsm:send_event_after(Delay * 1000, publish_last_will),
    State#state{delayed_will_timer = Ref}.

publish_last_will(#state{delayed_will = undefined} = State) ->
    State;
publish_last_will(#state{delayed_will = {_, Fun}} = State) ->
    Fun(),
    unset_will_timer(State#state{delayed_will = undefined}).

maybe_offline_store(Offline, SubscriberId, #deliver{qos=QoS, msg=#vmq_msg{persisted=false, dup=IsDup, expiry_ts=ExpiryTs} = Msg}=D) when QoS > 0 ->
    %% this function writes the message to the message store, in case the queue
    %% has no online session attached anymore (Offline = true) the queue can
    %% 'compress' the messages. Compressing is done by only keeping the message
    %% reference in the queue (the rest of the message is fetched from the
    %% message store during 'decompress'). However, the message store doesn't
    %% store all message properties, therefore the queue can't just 'compress'
    %% every message. The following properties aren't stored by the currently
    %% provided message store:
    %% - Message ID (required when storing a message with the DUP flag)
    %% - Expiry Timestamp (required when storing a message that should expire)
    PMsg = Msg#vmq_msg{persisted=true, qos=QoS},
    case vmq_message_store:write(SubscriberId, PMsg) of
        ok when Offline and IsDup ->
            % No compress
            D#deliver{msg=PMsg};
        ok when Offline and (ExpiryTs =/= undefined) ->
            % No compress
            D#deliver{msg=PMsg};
        ok when Offline ->
            % Compress
            PMsg#vmq_msg.msg_ref;
        ok ->
            % No compress, we're still online
            D#deliver{msg=PMsg};
        {error, _} ->
            %% in case we cannot store the message we keep the
            %% full message structure around
            D
    end;
maybe_offline_store(true, _, #deliver{msg=#vmq_msg{persisted=true, dup=true}} = D) ->
    % we can't compress a DUP message as we'd lose the message id when compressing
    D;
maybe_offline_store(true, _, #deliver{msg=#vmq_msg{persisted=true, expiry_ts=ExpiryTs}} = D) when ExpiryTs =/= undefined ->
    % we can't compress a message that has the expiry_ts set as we'd lose the expiry ts when compressing
    D;
maybe_offline_store(true, _, #deliver{msg=#vmq_msg{persisted=true} = Msg}) ->
    % Compress
    Msg#vmq_msg.msg_ref;
maybe_offline_store(_, _, MsgOrRef) -> MsgOrRef.

maybe_offline_delete(SubscriberId, #deliver{msg=#vmq_msg{persisted=true, msg_ref=MsgRef}}) ->
    _ = vmq_message_store:delete(SubscriberId, MsgRef),
    ok;
maybe_offline_delete(SubscriberId, MsgRef) when is_binary(MsgRef) ->
    _ = vmq_message_store:delete(SubscriberId, MsgRef),
    ok;
maybe_offline_delete(_, _) -> ok.

unset_timers(State) ->
    S1 = unset_expiry_timer(State),
    unset_will_timer(S1).

unset_will_timer(#state{delayed_will_timer=undefined} = State) -> State;
unset_will_timer(#state{delayed_will_timer=Ref} = State) ->
    gen_fsm:cancel_timer(Ref),
    State#state{delayed_will_timer=undefined}.

unset_expiry_timer(#state{expiry_timer=undefined} = State) -> State;
unset_expiry_timer(#state{expiry_timer=Ref} = State) ->
    gen_fsm:cancel_timer(Ref),
    State#state{expiry_timer=undefined}.

state_change(Msg, OldStateName, NewStateName) ->
    lager:debug("transition from ~p --> ~p because of ~p", [OldStateName, NewStateName, Msg]),
    NewStateName.

set_general_opts(#{queue_deliver_mode := DeliverMode,
                   max_offline_messages := MaxOfflineMsgs,
                   max_msgs_per_drain_step := MaxMsgsPerDrainStep},
                 #state{offline=Offline} = State) ->
    State#state{offline=Offline#queue{max=MaxOfflineMsgs},
                deliver_mode=DeliverMode,
                max_msgs_per_drain_step=MaxMsgsPerDrainStep}.

set_session_opts(SessionPid, #{max_online_messages := MaxOnlineMsgs,
                               queue_type := Type,
                               cleanup_on_disconnect := Clean}, #state{sessions=Sessions} = State) ->
    #session{queue=Queue} = Session = maps:get(SessionPid, Sessions),
    NewSessions = maps:update(SessionPid,
                              Session#session{
                                cleanup_on_disconnect=Clean,
                                queue=Queue#queue{
                                        max=MaxOnlineMsgs,
                                        type=Type
                                       }
                               }, Sessions),
    State#state{sessions=NewSessions}.

compress_queue(SId, #queue{queue=Q} = Queue) ->
    NewQueue = compress_queue(SId, queue:to_list(Q), []),
    Queue#queue{queue=NewQueue}.

compress_queue(_, [], Acc) ->
    queue:from_list(lists:reverse(Acc));
compress_queue(SId, [Msg|Rest], Acc) ->
    compress_queue(SId, Rest, [maybe_offline_store(true, SId, Msg)|Acc]).

decompress_queue(SId, #queue{queue=Q} = Queue) ->
    NewQueue = decompress_queue(SId, queue:to_list(Q), []),
    Queue#queue{queue=NewQueue}.

decompress_queue(_, [], Acc) ->
    queue:from_list(lists:reverse(Acc));
decompress_queue(SId, [MsgRef|Rest], Acc) when is_binary(MsgRef) ->
    case vmq_message_store:read(SId, MsgRef) of
        {ok, #vmq_msg{qos=QoS} = Msg} ->
            decompress_queue(SId, Rest,
                             [#deliver{qos=QoS, msg=Msg#vmq_msg{persisted=false}}|Acc]);
        {error, Reason} ->
            lager:warning("can't decompress queue item with msg_ref ~p for subscriber ~p due to ~p",
                          [MsgRef, SId, Reason]),
            decompress_queue(SId, Rest, Acc)
    end;
decompress_queue(SId, [#deliver{msg=Msg} = D|Rest], Acc) ->
    decompress_queue(SId, Rest, [D#deliver{msg=Msg#vmq_msg{persisted=false}}|Acc]);
decompress_queue(SId, [_|Rest], Acc) ->
    decompress_queue(SId, Rest, Acc).

queue_split(N, Queue) ->
    NN = case queue:len(Queue) of
             L when L > N -> N;
             L -> L
         end,
    queue:split(NN, Queue).

maybe_set_expiry_ts(#deliver{msg=#vmq_msg{expiry_ts={expire_after, ExpireAfter}} = Msg} = D) ->
    D#deliver{msg=Msg#vmq_msg{expiry_ts = {vmq_time:timestamp(second) + ExpireAfter, ExpireAfter}}};
maybe_set_expiry_ts(Msg) ->
    Msg.

on_message_drop_hook(SubscriberId, #deliver{msg=#vmq_msg{routing_key=RoutingKey, qos=QoS, payload=Payload, properties=Props}}, Reason) ->
    vmq_plugin:all(on_message_drop, [SubscriberId, fun() -> {RoutingKey, QoS, Payload, Props} end, Reason]);
on_message_drop_hook(SubscriberId, MsgRef, Reason) when is_binary(MsgRef) ->
    Promise = fun() ->
                      case vmq_message_store:read(SubscriberId, MsgRef) of
                          {ok, #vmq_msg{routing_key=RoutingKey, qos=QoS, payload=Payload, properties=Props}} ->
                              {RoutingKey, QoS, Payload, Props};
                          _ ->
                              error
                      end
              end,
    vmq_plugin:all(on_message_drop, [SubscriberId, Promise, Reason]).

maybe_expire_msgs(SId, Msgs) ->
    lists:filtermap(
      fun(Msg) ->
              case maybe_expire_msg(SId, Msg) of
                  expired -> false;
                  NewMsg -> {true, NewMsg}
              end
      end, Msgs).

maybe_expire_msg(_SId, #deliver{msg=#vmq_msg{expiry_ts=undefined}} = M) ->
    M;
maybe_expire_msg(SId,
                 #deliver{msg=#vmq_msg{expiry_ts={ExpiryTS, _}} = M} = D) ->
    case vmq_time:is_past(ExpiryTS) of
        true ->
            on_message_drop_hook(SId, D, expired),
            vmq_metrics:incr_queue_msg_expired(1),
            expired;
        Remaining ->
            D#deliver{msg=M#vmq_msg{expiry_ts={ExpiryTS, Remaining}}}
    end;
maybe_expire_msg(_SId, M) ->
    %%pubrels
    M.

to_internal({deliver, QoS, Msg}) ->
    #deliver{qos=QoS, msg=Msg};
to_internal(Msg) ->
    Msg.

to_external(#deliver{qos=QoS, msg=Msg}) ->
    {deliver, QoS, Msg}.


get_topics(SubscriberId) ->
    Subs = vmq_subscriber_db:read(SubscriberId),
    lists:flatmap(
        fun({_, _, NodeSubs}) ->
            lists:map(fun({Topic, _}) -> Topic end, NodeSubs)
        end,
    Subs).
