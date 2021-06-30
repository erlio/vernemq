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

-module(vmq_reg).
-include_lib("vmq_commons/include/vmq_types.hrl").
-include("vmq_server.hrl").

%% API
-export([
         %% used in mqtt fsm handling
         subscribe/3,
         unsubscribe/3,
         register_subscriber/5,
         register_subscriber_/5, %% used during testing
         replace_dead_queue/3,
         delete_subscriptions/1,
         %% used in mqtt fsm handling
         publish/4,

         %% used in :get_info/2
         get_session_pids/1,
         get_queue_pid/1,

         %% used in vmq_server_utils
         total_subscriptions/0,

         stored/1,
         status/1,

         prepare_offline_queue_migration/1,
         migrate_offline_queues/2,
         fix_dead_queues/2
        ]).

%% called by vmq_cluster_com
-export([route_remote_msg/4]).
%% used from plugins
-export([direct_plugin_exports/1,
         direct_plugin_exports/2]).
%% used by reg views
-export([subscribe_subscriber_changes/0,
         fold_subscriptions/2,
         fold_subscribers/2]).

%% exported because currently used by queue & netsplit tests
-export([subscriptions_for_subscriber_id/1]).

%% exported for testing purposes
-export([retain_pre/1]).

-record(publish_fold_acc, {
          msg                               :: msg(),
          subscriber_groups=undefined       :: undefined | map(),
          local_matches=0                   :: non_neg_integer(),
          remote_matches=0                  :: non_neg_integer()
         }).

-define(NR_OF_REG_RETRIES, 10).


-spec subscribe(flag(), subscriber_id(),
                [subscription()]) -> {ok, [qos() | not_allowed]} |
                                     {error, not_allowed | not_ready}.
subscribe(false, SubscriberId, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun subscribe_op/2, [SubscriberId, Topics]);
subscribe(true, SubscriberId, Topics) ->
    %% trade consistency for availability
    if_ready(fun subscribe_op/2, [SubscriberId, Topics]).

subscribe_op(SubscriberId, Topics) ->
    OldSubs = subscriptions_for_subscriber_id(SubscriberId),
    Existing = subscriptions_exist(OldSubs, Topics),
    add_subscriber(lists:usort(Topics), OldSubs, SubscriberId),
    QoSTable =
    lists:foldl(fun
                    %% MQTTv4 clauses
                    ({_, {_, not_allowed}}, AccQoSTable) ->
                        [not_allowed|AccQoSTable];
                    ({Exists, {T, QoS}}, AccQoSTable) when is_integer(QoS) ->
                        deliver_retained(SubscriberId, T, QoS, #{}, Exists),
                        [QoS|AccQoSTable];
                    %% MQTTv5 clauses
                    ({_, {_, {not_allowed, _}}}, AccQoSTable) ->
                        [not_allowed|AccQoSTable];
                    ({Exists, {T, {QoS, SubOpts}}}, AccQoSTable) when is_integer(QoS), is_map(SubOpts) ->
                        deliver_retained(SubscriberId, T, QoS, SubOpts, Exists),
                        [QoS|AccQoSTable]
                end, [], lists:zip(Existing,Topics)),
    {ok, lists:reverse(QoSTable)}.

-spec unsubscribe(flag(), subscriber_id(), [topic()]) -> ok | {error, not_ready}.
unsubscribe(false, SubscriberId, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun unsubscribe_op/2, [SubscriberId, Topics]);
unsubscribe(true, SubscriberId, Topics) ->
    %% trade consistency for availability
    unsubscribe_op( SubscriberId, Topics).

unsubscribe_op(SubscriberId, Topics) ->
    del_subscriptions(Topics, SubscriberId).

delete_subscriptions(SubscriberId) ->
    del_subscriber(SubscriberId).

-spec register_subscriber(flag(), flag(), subscriber_id(), boolean(), map()) ->
    {ok, #{initial_msg_id := msg_id(),
           session_present := flag(),
           queue_pid := pid()}} | {error, _}.
register_subscriber(AllowRegister, CoordinateRegs, SubscriberId, StartClean, #{allow_multiple_sessions := false} = QueueOpts) ->
    Netsplit = not vmq_cluster:is_ready(),
    %% we don't allow multiple sessions using same subscriber id
    %% allow_multiple_sessions is needed for session balancing
    SessionPid = self(),
    case {Netsplit, AllowRegister, CoordinateRegs} of
        {false, _, true} ->
            %% no netsplit, but coordinated registrations required.
            vmq_reg_sync:sync(SubscriberId,
                              fun() ->
                                      register_subscriber_(SessionPid, SubscriberId, StartClean,
                                                          QueueOpts, ?NR_OF_REG_RETRIES)
                              end, 60000);
        {true, false, _} ->
            %% netsplit, registrations during netsplits not allowed.
            {error, not_ready};
        _ ->
            %% all other cases we allow registrations but unsynced.
            register_subscriber_(SessionPid, SubscriberId, StartClean,
                                 QueueOpts, ?NR_OF_REG_RETRIES)
    end;
register_subscriber(CAPAllowRegister, _, SubscriberId, _StartClean, #{allow_multiple_sessions := true} = QueueOpts) ->
    %% we allow multiple sessions using same subscriber id
    %%
    %% !!! CleanSession is disabled if multiple sessions are in use
    %%
    case vmq_cluster:is_ready() or CAPAllowRegister of
        true ->
            register_session(SubscriberId, QueueOpts);
        false ->
            {error, not_ready}
    end.

register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N) ->
    register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N, register_subscriber_retry_exhausted).

-spec register_subscriber_(pid() | undefined, subscriber_id(), boolean(), map(), non_neg_integer(), any()) ->
    {'ok', map()} | {error, any()}.
register_subscriber_(_, _, _, _, 0, Reason) ->
    {error, Reason};
register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N, Reason) ->
    % wont create new queue in case it already exists
    {ok, QueuePresent, QPid} =
    case vmq_queue_sup_sup:start_queue(SubscriberId) of
        {ok, true, OldQPid} when StartClean ->
            %% cleanup queue
            vmq_queue:cleanup(OldQPid, ?SESSION_TAKEN_OVER),
            vmq_queue_sup_sup:start_queue(SubscriberId);
        Ret -> Ret
    end,
    case vmq_queue:info(QPid) of
        #{statename := drain} ->
            %% queue is draining it's offline queue to a different
            %% remote queue. This can happen if a client hops around
            %% different nodes very frequently... adjust load balancing!!
            timer:sleep(100),
            %% retry and see if it is finished soon before trying to
            %% remap the subscriber.
            register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N -1, {register_subscriber_retry_exhausted, draining});
        _ ->
            %% remap subscriber... enabling that new messages will
            %% eventually reach the new queue.  Remapping triggers
            %% remote nodes to initiate queue migration
            {SubscriptionsPresent, UpdatedSubs, ChangedNodes}
                = maybe_remap_subscriber(SubscriberId, StartClean),
            SessionPresent1 = SubscriptionsPresent or QueuePresent,
            SessionPresent2 =
                case StartClean of
                    true ->
                        false; %% SessionPresent is always false in case CleanupSession=true
                    false when QueuePresent ->
                        %% no migration expected to happen, as queue is already local.
                        SessionPresent1;
                    false ->
                        Fun = fun(Sid, OldNode) ->
                                      case rpc:call(OldNode, ?MODULE, get_queue_pid, [Sid]) of
                                          {badrpc, nodedown} ->
                                              lager:warning("get_queue_pid on old node ~p failed : intercepted {badrpc, nodedown}", [OldNode]),
                                              done;
                                          not_found ->
                                              case get_queue_pid(Sid) of
                                                  not_found ->
                                                      block;
                                                  LocalPid when is_pid(LocalPid) ->
                                                      done
                                              end;
                                          OldPid when is_pid(OldPid) ->
                                              case vmq_queue:info(OldPid) of
                                                  #{statename := drain} ->
                                                      %% Queue is in draining state, we're done here and
                                                      %% can return to the caller.
                                                      %% TODO: We can improve this by only having a single
                                                      %% rpc call. But this would break backward upgrade
                                                      %% compatibility.
                                                      done;
                                                  _ ->
                                                      block
                                              end
                                      end
                              end,
                        block_until(SubscriberId, UpdatedSubs, ChangedNodes, Fun),
                        SessionPresent1
                end,
            case catch vmq_queue:add_session(QPid, SessionPid, QueueOpts) of
                {'EXIT', {normal, _}} ->
                    %% queue went down in the meantime, retry
                    register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N -1, register_subscriber_retry_exhausted);
                {'EXIT', {noproc, _}} ->
                    timer:sleep(100),
                    %% queue was stopped in the meantime, retry
                    register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N -1, register_subscriber_retry_exhausted);
                {'EXIT', Reason} ->
                    {error, Reason};
                {error, draining} ->
                    %% queue is still draining it's offline queue to a different
                    %% remote queue. This can happen if a client hops around
                    %% different nodes very frequently... adjust load balancing!!
                    timer:sleep(100),
                    register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N -1, {register_subscriber_retry_exhausted, draining});
                {error, {cleanup, _Reason}} ->
                    %% queue is still cleaning up.
                    timer:sleep(100),
                    register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N -1, register_subscriber_retry_exhausted);
                {ok, Opts} ->
                    {ok, Opts#{session_present => SessionPresent2,
                               queue_pid => QPid}}
            end
    end.



%% block_until/4 has three cases to consider, the logic for
%% these cases are handled by the BlockCond function
%%
%% migrate queue to local node (register_subscriber):
%%
%%    we wait until there is
%%      "the original queue has been terminated" OR "the original queue is indraining state"
%%
%% migrate local queue to remote node (cluster leave):
%%
%%    we wait until there is no local queue and until there is one on the remote node.
%%
%% migrate dead queue (node down) to another node in cluster (including this one):
%%
%%    we have no local queue, but wait until the (offline) queue exists on target node.
block_until(_, _, [], _) -> ok;
block_until(SubscriberId, UpdatedSubs, [Node|Rest] = ChangedNodes, BlockCond) ->
    %% the call to subscriptions_for_subscriber_id will resolve any remaining
    %% conflicts to this entry by broadcasting the resolved value to the
    %% other nodes
    case subscriptions_for_subscriber_id(SubscriberId) of
        UpdatedSubs -> ok;
        _ ->
            %% in case the subscriptions were resolved elsewhere in the meantime
            %% we'll write 'our' version of the remapped subscriptions
            vmq_subscriber_db:store(SubscriberId, UpdatedSubs)
    end,

    case BlockCond(SubscriberId, Node) of
        block ->
            timer:sleep(100),
            block_until(SubscriberId, UpdatedSubs, ChangedNodes, BlockCond);
        done ->
            block_until(SubscriberId, UpdatedSubs, Rest, BlockCond)
    end.

-spec register_session(subscriber_id(), map()) -> {ok, #{initial_msg_id := msg_id(),
                                                         session_present := flag(),
                                                         queue_pid := pid()}}.
register_session(SubscriberId, QueueOpts) ->
    %% register_session allows to have multiple subscribers connected
    %% with the same session_id (as oposed to register_subscriber)
    SessionPid = self(),
    {ok, QueuePresent, QPid} = vmq_queue_sup_sup:start_queue(SubscriberId), % wont create new queue in case it already exists
    {ok, SessionOpts} = vmq_queue:add_session(QPid, SessionPid, QueueOpts),
    %% TODO: How to handle SessionPresent flag for allow_multiple_sessions=true
    SessionPresent = QueuePresent,
    {ok, SessionOpts#{session_present => SessionPresent,
                      queue_pid => QPid}}.

%% publish/4
-spec publish(flag(), module(), client_id() | ?INTERNAL_CLIENT_ID, msg()) -> {ok, {integer(), integer()}} | {'error', _}.
publish(true, RegView, ClientId, #vmq_msg{mountpoint=MP,
                                          routing_key=Topic,
                                          payload=Payload,
                                          retain=IsRetain,
                                          properties=Properties} = Msg) ->
    %% trade consistency for availability
    %% if the cluster is not consistent at the moment, it is possible
    %% that subscribers connected to other nodes won't get this message
    case IsRetain of
        true when Payload == <<>> ->
            %% retain delete action
            vmq_retain_srv:delete(MP, Topic),
            publish_fold_wrapper(RegView, ClientId, Topic, Msg);
        true ->
            %% retain set action
            vmq_retain_srv:insert(MP, Topic, #retain_msg{
                                                payload = Payload,
                                                properties = Properties,
                                                expiry_ts = maybe_set_expiry_ts(Properties)
                                               }),
            publish_fold_wrapper(RegView, ClientId, Topic, Msg);
        false ->
            publish_fold_wrapper(RegView, ClientId, Topic, Msg)
    end;
publish(false, RegView, ClientId, #vmq_msg{mountpoint=MP,
                                               routing_key=Topic,
                                               payload=Payload,
                                               properties=Properties,
                                               retain=IsRetain} = Msg) ->
    %% don't trade consistency for availability
    case vmq_cluster:is_ready() of
        true when (IsRetain == true) and (Payload == <<>>) ->
            %% retain delete action
            vmq_retain_srv:delete(MP, Topic),
            publish_fold_wrapper(RegView, ClientId, Topic, Msg);
        true when (IsRetain == true) ->
            %% retain set action
            vmq_retain_srv:insert(MP, Topic, #retain_msg{
                                                payload = Payload,
                                                properties = Properties,
                                                expiry_ts = maybe_set_expiry_ts(Properties)
                                               }),
            publish_fold_wrapper(RegView, ClientId, Topic, Msg);
        true ->
            publish_fold_wrapper(RegView, ClientId, Topic, Msg);
        false ->
            {error, not_ready}
    end.

% route_remote_msg/4 is called by the vmq_cluster_com
-spec route_remote_msg(module(), mountpoint(), topic(), msg()) -> ok.
route_remote_msg(RegView, MP, Topic, Msg) ->
    SubscriberId = {MP, ?INTERNAL_CLIENT_ID},
    Acc = #publish_fold_acc{msg=Msg},
    _ = vmq_reg_view:fold(RegView, SubscriberId, Topic, fun route_remote_msg_fold_fun/3, Acc),
    % don't increment the router_matches_[local|remote] here, as they're already counted
    % at the origin node.
    ok.
route_remote_msg_fold_fun({_, _} = SubscriberIdAndSubInfo, From, Acc) ->
    publish_fold_fun(SubscriberIdAndSubInfo, From, Acc);
route_remote_msg_fold_fun(Node, From, Acc) ->
    %% When this node restarted after e.g a crash, messages buffered in remote node
    %% will arrive here and need to be handled.
    publish_fold_fun(Node, From, Acc),
    Acc.

-spec publish_fold_wrapper(module(), client_id(), topic(), msg()) -> {ok, {integer(), integer()}}.
publish_fold_wrapper(RegView, ClientId, Topic, #vmq_msg{sg_policy = SGPolicy,
                                                        mountpoint = MP} = Msg) ->
    Acc = #publish_fold_acc{msg=Msg},
    #publish_fold_acc{msg=NewMsg,
                      subscriber_groups=SubscriberGroups,
                      local_matches=LocalMatches0,
                      remote_matches=RemoteMatches0} = vmq_reg_view:fold(RegView, {MP, ClientId}, Topic, fun publish_fold_fun/3, Acc),
    {LocalMatches1, RemoteMatches1} = vmq_shared_subscriptions:publish(NewMsg, SGPolicy, SubscriberGroups, LocalMatches0, RemoteMatches0),
    vmq_metrics:incr_router_matches_local(LocalMatches1),
    vmq_metrics:incr_router_matches_remote(RemoteMatches1),
    {ok, {LocalMatches1, RemoteMatches1}}.

%% publish_fold_fun/3 is used as the fold function in RegView:fold/4
publish_fold_fun({SubscriberId, {_, #{no_local := true}}}, SubscriberId, Acc) ->
    %% Publisher is the same as subscriber, discard.
    Acc;
publish_fold_fun({{_,_} = SubscriberId, SubInfo}, _FromClientId, #publish_fold_acc{msg=Msg0,
                                                                                   local_matches=N} = Acc) ->
    case get_queue_pid(SubscriberId) of
        not_found -> Acc;
        QPid ->
            Msg1 = handle_rap_flag(SubInfo, Msg0),
            Msg2 = maybe_add_sub_id(SubInfo, Msg1),
            QoS = qos(SubInfo),
            ok = vmq_queue:enqueue(QPid, {deliver, QoS, Msg2}),
            Acc#publish_fold_acc{local_matches= N + 1}
    end;
publish_fold_fun({_Node, _Group, SubscriberId, #{no_local := true}}, SubscriberId, Acc) ->
    %% Publisher is the same as subscriber, discard.
    Acc;
publish_fold_fun({_Node, _Group, _SubscriberId, _SubInfo} = Sub, _FromClientId, #publish_fold_acc{
                                                                                   msg=#vmq_msg{sg_policy=SGPolicy},
                                                                                   subscriber_groups=SubscriberGroups} = Acc) ->
    %% collect subscriber group members for later processing
    Acc#publish_fold_acc{subscriber_groups=add_to_subscriber_group(Sub, SubscriberGroups, SGPolicy)};
publish_fold_fun(Node, _FromClientId, #publish_fold_acc{msg=Msg, remote_matches=N} = Acc) ->
    case vmq_cluster:publish(Node, Msg) of
        ok ->
            Acc#publish_fold_acc{remote_matches= N + 1};
        {error, Reason} ->
            lager:warning("can't publish to remote node ~p due to '~p'", [Node, Reason]),
            Acc
    end.

maybe_set_expiry_ts(#{p_message_expiry_interval := ExpireAfter}) ->
    {vmq_time:timestamp(second) + ExpireAfter, ExpireAfter};
maybe_set_expiry_ts(_) ->
    undefined.

-spec handle_rap_flag(subinfo(), msg()) -> msg().
handle_rap_flag({_QoS, #{rap := true}}, Msg) ->
    Msg;
handle_rap_flag(_SubInfo, Msg) ->
    %% Default is to set the retain flag to false to be compatible with MQTTv3
    Msg#vmq_msg{retain = false}.

maybe_add_sub_id({_, #{sub_id := SubId}}, #vmq_msg{properties = Props} = Msg) ->
    Msg#vmq_msg{properties = Props#{p_subscription_id => [SubId]}};
maybe_add_sub_id(_, Msg) ->
    Msg.

-spec qos(subinfo()) -> qos().
qos({QoS, _}) when is_integer(QoS) ->
    QoS;
qos(QoS) when is_integer(QoS) ->
    QoS.

add_to_subscriber_group(Sub, undefined, SGPolicy) ->
    add_to_subscriber_group(Sub, #{}, SGPolicy);
add_to_subscriber_group({Node, _Group, _SubscriberId, _SubInfo}, SubscriberGroups, local_only) when Node =/= node()->
    SubscriberGroups;
add_to_subscriber_group({Node, Group, SubscriberId, SubInfo}, SubscriberGroups, _SGPolicy) ->
    SubscriberGroup = maps:get(Group, SubscriberGroups, []),
    maps:put(Group, [{Node, SubscriberId, SubInfo}|SubscriberGroup],
             SubscriberGroups).


-spec deliver_retained(subscriber_id(), topic(), qos(), subopts(), boolean()) -> 'ok'.
deliver_retained(_, _, _, #{retain_handling := dont_send}, _) ->
    %% don't send, skip
    ok;
deliver_retained(_, _, _, #{retain_handling := send_if_new_sub}, true) ->
    %% subscription already existed, skip.
    ok;
deliver_retained(_SubscriberId, [<<"$share">>|_], _QoS, _SubOpts, _) ->
    %% Never deliver retained messages to subscriber groups.
    ok;
deliver_retained({MP, _} = SubscriberId, Topic, QoS, SubOpts, _) ->
    QPid = get_queue_pid(SubscriberId),
    vmq_retain_srv:match_fold(
      fun ({T, #retain_msg{payload = Payload,
                           properties = Properties,
                           expiry_ts = ExpiryTs}}, _) ->
              Msg = #vmq_msg{routing_key=T,
                             payload=retain_pre(Payload),
                             retain=true,
                             qos=QoS,
                             dup=false,
                             mountpoint=MP,
                             msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                             expiry_ts = ExpiryTs,
                             properties=Properties},
              Msg1 = maybe_add_sub_id({QoS,SubOpts}, Msg),
              maybe_delete_expired(ExpiryTs, MP, Topic),
              vmq_queue:enqueue(QPid, {deliver, QoS, Msg1});
          ({T, Payload}, _) when is_binary(Payload) ->
              %% compatibility with old style retained messages.
              Msg = #vmq_msg{routing_key=T,
                             payload=retain_pre(Payload),
                             retain=true,
                             qos=QoS,
                             dup=false,
                             mountpoint=MP,
                             msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                             properties=#{}},
              vmq_queue:enqueue(QPid, {deliver, QoS, Msg})
      end, ok, MP, Topic).

maybe_delete_expired(undefined, _, _) -> ok;
maybe_delete_expired({Ts, _}, MP, Topic) ->
    case vmq_time:is_past(Ts) of
        true ->
            vmq_retain_srv:delete(MP, Topic);
        _ ->
            ok
    end.

subscriptions_for_subscriber_id(SubscriberId) ->
    Default = [],
    vmq_subscriber_db:read(SubscriberId, Default).

prepare_offline_queue_migration([]) -> exit(no_target_available);
prepare_offline_queue_migration(Targets) ->
    Acc = #{offline_cnt => 0,
            offline_queues => [],
            draining_cnt => 0,
            draining_queues => [],
            total_queue_count => 0,
            total_msg_count => 0,
            migrated_queue_cnt => 0,
            migrated_msg_cnt => 0,
            targets => Targets},
    vmq_queue_sup_sup:fold_queues(fun migration_candidate_filter/3,  Acc).

migrate_offline_queues(#{offline_queues := [], draining_queues := []} = State, _) ->
    {done, State};
migrate_offline_queues(S0, MaxConcurrency) ->
    S1 = update_state(S0),
    S2 = trigger_migration(S1, MaxConcurrency),
    {cont, S2}.

migration_candidate_filter(SubscriberId, QPid, #{offline_cnt := OCnt,
                                                 offline_queues := OQueues,
                                                 draining_cnt := DCnt,
                                                 draining_queues := DQueues,
                                                 targets := Targets,
                                                 total_queue_count := TQCnt,
                                                 total_msg_count := TMCnt} = Acc) ->
    Target = lists:nth(rand:uniform(length(Targets)), Targets),
    try vmq_queue:status(QPid) of
        {_, _, _, _, true} ->
            %% this is a queue belonging to a plugin.. ignore it.
            Acc;
        {offline, _, Msgs, _, _} ->
            Acc#{offline_cnt => OCnt + 1,
                 offline_queues => [{SubscriberId,QPid,Msgs,Target}|OQueues],
                 total_queue_count := TQCnt + 1,
                 total_msg_count := TMCnt + Msgs};
        {drain, _, Msgs, _, _} ->
            Acc#{draining_cnt => DCnt + 1,
                 draining_queues => [{SubscriberId,QPid,Msgs,Target}|DQueues],
                 total_queue_count => TQCnt + 1,
                 total_msg_count => TMCnt + Msgs};
        _ ->
            Acc
    catch
        _:_ ->
            %% queue stopped in the meantime, that's ok.
            Acc
    end.

update_state(#{draining_queues := DrainingQueues} = S0) ->
    lists:foldl(
      fun({_, QPid, Msgs, _} = Q, #{offline_cnt := OCnt,
                                    offline_queues := OQueues,
                                    draining_cnt := DCnt,
                                    draining_queues := DQueues,
                                    migrated_queue_cnt := MigratedQueueCnt,
                                    migrated_msg_cnt := MigratedMsgCnt} = Acc) ->
              try vmq_queue:status(QPid) of
                  {offline, _, _, _, _} ->
                      %% queue returned to offline state!
                      Acc#{offline_cnt => OCnt + 1,
                           offline_queues => [Q|OQueues]};
                  {drain, _, _, _, _} ->
                      %% still draining
                      Acc#{draining_cnt => DCnt + 1,
                           draining_queues => [Q|DQueues]}
              catch
                  _:_ ->
                      %% queue stopped in the meantime, so draining
                      %% finished.
                      Acc#{migrated_queue_cnt := MigratedQueueCnt + 1,
                           migrated_msg_cnt := MigratedMsgCnt + Msgs}
              end
      end, S0#{draining_queues => [],
               draining_cnt => 0}, DrainingQueues).

trigger_migration(#{draining_cnt := DCnt} = S, MaxConcurrency) when DCnt >= MaxConcurrency->
    %% all are still draining and we may even more than max
    %% concurrency draining queues due to natural migration. Do
    %% nothing.
    S;
trigger_migration(#{offline_queues := []} = S, _) ->
    %% done for now
    S;
trigger_migration(#{draining_cnt := DCnt,
                    draining_queues := DQueues,
                    offline_cnt := OCnt,
                    offline_queues := [{SubscriberId, _QPid, _Msgs, Target}=Q|OQueues]} = S,
                  MaxConcurrency) ->
    %% Remap Subscriptions, taking into account subscriptions
    %% on other nodes by only remapping subscriptions on 'OldNode'
    OldNode = node(),
    Subs = subscriptions_for_subscriber_id(SubscriberId),
    UpdatedSubs = vmq_subscriber:change_node(Subs, OldNode, Target, false),
    vmq_subscriber_db:store(SubscriberId, UpdatedSubs),
    S1 = S#{draining_queues => [Q|DQueues], draining_cnt => DCnt + 1,
            offline_queues => OQueues, offline_cnt => OCnt - 1},
    trigger_migration(S1, MaxConcurrency).

fix_dead_queues(_, []) -> exit(no_target_available);
fix_dead_queues(DeadNodes, AccTargets) ->
    %% The goal here is to:
    %%
    %% 1. create queues on another node for all clean_start = false
    %% sessions.
    %%
    %% 2. Purge the old nodename from the rest of the metadata to
    %% ensure publishes are not forwarded to the dead node.

    %% DeadNodes must be a list of offline VerneMQ nodes. Targets must
    %% be a list of online VerneMQ nodes
    {_, _, N} = fold_subscribers(fun fix_dead_queue/3, {DeadNodes, AccTargets, 0}),
    lager:info("dead queues summary: ~p queues fixed", [N]).

fix_dead_queue(SubscriberId, Subs, {DeadNodes, [Target|Targets], N}) ->
    %%% Why not use maybe_remap_subscriber/2:
    %%%  it is possible that the original subscriber has used
    %%%  allow_multiple_sessions=true
    %%%
    %%%  we only remap the subscriptions on dead nodes
    %%%  and ensure that a queue exist for such subscriptions.
    %%%  In case allow_multiple_sessions=false (default) all
    %%%  subscriptions will be remapped

    %% check if there's any work to do:
    NewSubs = rewrite_dead_nodes(Subs, DeadNodes, '$notanodename', false),
    case Subs of
        NewSubs ->
            %% no subs were changed, nothing to do here.
            {DeadNodes, [Target|Targets], N};
        _ ->
            %% Assume all the subscriptions come with the same clean_session flag.
            [{_, StartClean, _}|_] = NewSubs,
            RepairQueueFun =
                fun() ->
                        case rpc:call(Target, vmq_reg, replace_dead_queue, [SubscriberId, DeadNodes, StartClean]) of
                            ok ->
                                {DeadNodes, Targets ++ [Target], N + 1};
                            Error ->
                                lager:info("repairing dead queue for ~p on ~p failed due to ~p~n", [SubscriberId, Target, Error]),
                                {DeadNodes, Targets ++ [Target], N}
                        end
                end,
            case vmq_reg_sync:sync(SubscriberId, RepairQueueFun, 60000) of
                {error, Error} ->
                    lager:info("repairing dead queue for ~p on ~p failed due to ~p~n", [SubscriberId, Target, Error]),
                    {DeadNodes, Targets ++ [Target], N};
                {_,_,_} = Acc -> Acc
            end
    end.


replace_dead_queue(SubscriberId, _DeadNodes, _StartClean = true) ->
    %% At this point we may have a new subscriber on either this or
    %% other nodes which may have another set of subscriptions than
    %% what the old node had.

    case get_queue_pid(SubscriberId) of
        not_found ->
            %% To ensure we end up with a client which has a consistent state,
            %% we write an empty subscription to the store which will cause
            %% clients on other nodes to be disconnected (if
            %% allow_multiple_sessions=false) (TODO: formalize this behaviour
            %% in a test-case) and they'll then reconnect.
            Subs = vmq_subscriber:new(true),
            vmq_subscriber_db:store(SubscriberId, Subs),
            %% no local queue, so we delete the client information.
            del_subscriber(SubscriberId),
            ok;
        QPid ->
            %% we force a disconnect to ensure the client reconnects
            %% and reestablishes consistent metadata.
            vmq_queue:force_disconnect(QPid, ?ADMINISTRATIVE_ACTION, true),
            ok
    end;
replace_dead_queue(SubscriberId, DeadNodes, _StartClean = false) ->
    %% At this point we may have a new subscriber on either this or
    %% other nodes and their subscribtions may differ from the ones we
    %% saw on the caller node.

    %% here the goal is that there is a (new) queue to receive any
    %% offline messages until the client reconnects.
    {ok, _, _QPid} = vmq_queue_sup_sup:start_queue(SubscriberId),
    case vmq_subscriber_db:read(SubscriberId) of
        undefined ->
            %% not clear why we ended up here - better return
            %% an error and let the caller retry.
            error;
        LocalSubs ->
            NewSubs = rewrite_dead_nodes(LocalSubs, DeadNodes, node(), false),
            %% store the updated subs, also to make sure to
            %% propagate the new values to all other nodes.
            vmq_subscriber_db:store(SubscriberId, NewSubs)
    end.

rewrite_dead_nodes(Subs, DeadNodes, TargetNode, CleanSession) ->
    lists:foldl(
      fun(DeadNode, AccSubs) ->
              %% Remapping the supbscriptions from DeadNode to
              %% TargetNode. In case a 'new' session is already
              %% present at TargetNode, we're merging the subscriptions
              %% IF it is using clean_session=false. IF it is using
              %% clean_session=true, the subscriptions are replaced.
              vmq_subscriber:change_node(AccSubs, DeadNode, TargetNode, CleanSession)
      end, Subs, DeadNodes).

-spec wait_til_ready() -> 'ok'.
wait_til_ready() ->
    case catch vmq_cluster:if_ready(fun() -> true end, []) of
        true ->
            ok;
        _ ->
            timer:sleep(100),
            wait_til_ready()
    end.

-spec direct_plugin_exports(module()) -> {function(), function(), {function(), function()}} | {error, invalid_config}.
direct_plugin_exports(Mod) when is_atom(Mod) ->
    %% This Function exports a generic Register, Publish, and Subscribe
    %% Fun, that a plugin can use if needed. Currently all functions
    %% block until the cluster is ready.
    CAPPublish = vmq_config:get_env(allow_publish_during_netsplit, false),
    CAPSubscribe = vmq_config:get_env(allow_subscribe_during_netsplit, false),
    CAPUnsubscribe = vmq_config:get_env(allow_unsubscribe_during_netsplit, false),
    SGPolicyConfig = vmq_config:get_env(shared_subscription_policy, prefer_local),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    {ok, #{register_fun := RegFun,
           publish_fun := PubFun,
           subscribe_fun := SubFun,
           unsubscribe_fun := UnsubFun}} =
        direct_plugin_exports(Mod, #{mountpoint => "",
                                     cap_publish => CAPPublish,
                                     cap_subscribe => CAPSubscribe,
                                     cap_unsubscribe => CAPUnsubscribe,
                                     sg_policy => SGPolicyConfig,
                                     reg_view => RegView}),
    {RegFun, PubFun, {SubFun, UnsubFun}}.

-spec direct_plugin_exports(module(), map()) ->
   {ok, #{client_id := client_id(),
          register_fun := function(),
          publish_fun := function(),
          subscribe_fun := function(),
          unsubscribe_fun := function()}}.
direct_plugin_exports(LogName, Opts) ->
    CAPPublish = maps:get(cap_publish, Opts, false),
    CAPSubscribe = maps:get(cap_subscribe, Opts, false),
    CAPUnsubscribe = maps:get(cap_unsubscribe, Opts, false),
    SGPolicy = maps:get(sg_policy, Opts, prefer_local),
    Mountpoint = maps:get(mountpoint, Opts, ""),
    RegView = maps:get(reg_view, Opts, vmq_reg_trie),

    CallingPid = self(),
    ClientIdDef = list_to_binary(
                    base64:encode_to_string(
                      integer_to_binary(
                        erlang:phash2(CallingPid)))),
    ClientId = maps:get(client_id, Opts, ClientIdDef),
    SubscriberId = {Mountpoint, ClientId},
    MaybeWaitTillReady =
        case maps:get(wait_till_ready, Opts, undefined) of
            undefined -> fun wait_til_ready/0;
            true -> fun wait_til_ready/0;
            false -> fun() -> ok end
        end,
    RegisterFun =
    fun() ->
            PluginPid = self(),
            MaybeWaitTillReady(),
            PluginSessionPid = spawn_link(
                                 fun() ->
                                         monitor(process, PluginPid),
                                         vmq_mqtt_fsm_util:plugin_receive_loop(PluginPid, LogName)
                                 end),
            QueueOpts = maps:merge(vmq_queue:default_opts(),
                                   #{cleanup_on_disconnect => true,
                                     is_plugin => true}),
            {ok, _} = register_subscriber_(PluginSessionPid, SubscriberId, true,
                                           QueueOpts, ?NR_OF_REG_RETRIES),
            ok
    end,

    PublishFun =
    fun([W|_] = Topic, Payload, Opts_) when is_binary(W)
                                            and is_binary(Payload)
                                            and is_map(Opts_) ->
            MaybeWaitTillReady(),
            %% allow a plugin developer to override
            %% - mountpoint
            %% - dup flag
            %% - retain flag
            %% - trade-consistency flag
            %% - reg_view
            %% - shared subscription policy
            Msg = #vmq_msg{
                     routing_key=Topic,
                     mountpoint=maps:get(mountpoint, Opts_, Mountpoint),
                     payload=Payload,
                     msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                     qos = maps:get(qos, Opts_, 0),
                     dup=maps:get(dup, Opts_, false),
                     retain=maps:get(retain, Opts_, false),
                     sg_policy=maps:get(shared_subscription_policy, Opts_, SGPolicy)
                    },
            publish(CAPPublish, RegView, ClientId, Msg)
    end,

    SubscribeFun =
    fun([W|_] = Topic) when is_binary(W) ->
            MaybeWaitTillReady(),
            CallingPid = self(),
            subscribe(CAPSubscribe, {Mountpoint, ClientId}, [{Topic, 0}]);
       (_) ->
            {error, invalid_topic}
    end,

    UnsubscribeFun =
    fun([W|_] = Topic) when is_binary(W) ->
            MaybeWaitTillReady(),
            CallingPid = self(),
            unsubscribe(CAPUnsubscribe, {Mountpoint, ClientId}, [Topic]);
       (_) ->
            {error, invalid_topic}
    end,
    {ok, #{register_fun => RegisterFun,
           publish_fun => PublishFun,
           subscribe_fun => SubscribeFun,
           unsubscribe_fun => UnsubscribeFun,
           client_id => ClientId}}.


subscribe_subscriber_changes() ->
    vmq_subscriber_db:subscribe_db_events().

fold_subscriptions(FoldFun, Acc) ->
    fold_subscribers(
      fun ({MP, _} = SubscriberId, Subs, AAcc) ->
              vmq_subscriber:fold(
                fun({Topic, QoS, Node}, AAAcc) ->
                        FoldFun({MP, Topic, {SubscriberId, QoS, Node}}, AAAcc)
                end, AAcc, Subs)
      end, Acc).

fold_subscribers(FoldFun, Acc) ->
    vmq_subscriber_db:fold(
      fun ({SubscriberId, Subs}, AccAcc) ->
              FoldFun(SubscriberId, Subs, AccAcc)
      end, Acc).

-spec add_subscriber([{topic(), qos() | not_allowed} |
                      {topic(), {qos() | not_allowed, map()}}],
                     vmq_subscriber:subs(), subscriber_id()) -> ok.
add_subscriber(Topics, OldSubs, SubscriberId) ->
    NewSubs =
        lists:filter(
          fun({_T, QoS}) when is_integer(QoS) ->
                  true;
             ({_T, {QoS, _Opts}}) when is_integer(QoS) ->
                  true;
             (_) -> false
          end, Topics),
    case vmq_subscriber:add(OldSubs, NewSubs) of
        {NewSubs0, true} ->
            vmq_subscriber_db:store(SubscriberId, NewSubs0);
        _ ->
            ok
    end.

-spec subscriptions_exist(vmq_subscriber:subs(), [topic()]) -> [boolean()].
subscriptions_exist(OldSubs, Topics) ->
    [vmq_subscriber:exists(Topic, OldSubs) || {Topic, _} <- Topics].

-spec del_subscriber(subscriber_id()) -> ok.
del_subscriber(SubscriberId) ->
    vmq_subscriber_db:delete(SubscriberId).

-spec del_subscriptions([topic()], subscriber_id()) -> ok.
del_subscriptions(Topics, SubscriberId) ->
    OldSubs = subscriptions_for_subscriber_id(SubscriberId),
    case vmq_subscriber:remove(OldSubs, Topics) of
        {NewSubs, true} ->
            vmq_subscriber_db:store(SubscriberId, NewSubs);
        _ ->
            ok
    end.

%% the return value is used to inform the caller
%% if a session was already present for the given
%% subscriber id.
-spec maybe_remap_subscriber(subscriber_id(), boolean()) ->
    {boolean(), undefined | vmq_subscriber:subs(), [node()]}.
maybe_remap_subscriber(SubscriberId, _StartClean = true) ->
    %% no need to remap, we can delete this subscriber
    %% we overwrite any other value
    Subs = vmq_subscriber:new(true),
    vmq_subscriber_db:store(SubscriberId, Subs),
    {false, Subs, []};
maybe_remap_subscriber(SubscriberId, _StartClean = false) ->
    case vmq_subscriber_db:read(SubscriberId) of
        undefined ->
            %% Store empty Subscriber Data
            Subs = vmq_subscriber:new(false),
            vmq_subscriber_db:store(SubscriberId, Subs),
            {false, Subs, []};
        Subs ->
            case vmq_subscriber:change_node_all(Subs, node(), false) of
                {NewSubs, ChangedNodes} when length(ChangedNodes) > 0 ->
                    vmq_subscriber_db:store(SubscriberId, NewSubs),
                    {true, NewSubs, ChangedNodes};
                _ ->
                    {true, Subs, []}
            end
    end.

-spec get_session_pids(subscriber_id()) ->
    {'error','not_found'} | {'ok', pid(), [pid()]}.
get_session_pids(SubscriberId) ->
    case get_queue_pid(SubscriberId) of
        not_found ->
            {error, not_found};
        QPid ->
            Pids = vmq_queue:get_sessions(QPid),
            {ok, QPid, Pids}
    end.

-spec get_queue_pid(subscriber_id()) -> pid() | not_found.
get_queue_pid(SubscriberId) ->
    vmq_queue_sup_sup:get_queue_pid(SubscriberId).

total_subscriptions() ->
    Total =
    vmq_subscriber_db:fold(
      fun({_, Subs}, Acc) ->
              vmq_subscriber:fold(fun(_, AccAcc) ->
                                          AccAcc + 1
                                  end, Acc, Subs)
      end, 0),
    [{total, Total}].

stored(SubscriberId) ->
    case get_queue_pid(SubscriberId) of
        not_found -> 0;
        QPid ->
            {_, _, Queued, _, _} = vmq_queue:status(QPid),
            Queued
    end.

status(SubscriberId) ->
    case get_queue_pid(SubscriberId) of
        not_found -> {error, not_found};
        QPid ->
            {ok, vmq_queue:status(QPid)}
    end.

%% retain, pre-versioning
%% {MPTopic, MsgOrDeleted}
%% future version:
%% {retain_msg, version, payload, properties, expiry_ts, ...}

-define(RETAIN_PRE_V, 0).

-spec retain_pre(binary() | tuple()) -> binary().
retain_pre(Msg) when is_binary(Msg) ->
    Msg;
retain_pre(FutureRetain) when is_tuple(FutureRetain),
                              element(1, FutureRetain) =:= retain_msg,
                              is_integer(element(2, FutureRetain)),
                              element(2, FutureRetain) > ?RETAIN_PRE_V ->
    element(3, FutureRetain).


-spec if_ready(_, _) -> any().
if_ready(Fun, Args) ->
    case persistent_term:get(subscribe_trie_ready, 0) of
        1 ->
            apply(Fun, Args);
        0 ->
            {error, not_ready}
    end.
