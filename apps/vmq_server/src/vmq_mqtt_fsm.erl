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

-module(vmq_mqtt_fsm).
-include_lib("vmq_commons/include/vmq_types.hrl").
-include("vmq_server.hrl").
-include("vmq_metrics.hrl").


-export([init/3,
         data_in/2,
         msg_in/2,
         info/2]).

-define(IS_PROTO_4(X), X =:= 4; X =:= 132).
-define(IS_PROTO_3(X), X =:= 3; X =:= 131).
-define(IS_BRIDGE(X), X =:= 131; X =:= 132).

-type timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-record(cap_settings, {
         allow_register = false    :: boolean(),
         allow_publish = false     :: boolean(),
         allow_subscribe = false   :: boolean(),
         allow_unsubscribe = false   :: boolean()
         }).
-type cap_settings() :: #cap_settings{}.

-record(state, {
          %% mqtt layer requirements
          next_msg_id=undefined             :: undefined | msg_id(),
          subscriber_id                     :: undefined | subscriber_id() | {mountpoint(), undefined},
          will_msg                          :: undefined | msg(),
          waiting_acks=maps:new()           :: map(),
          waiting_msgs=[]                   :: list(),
          %% auth backend requirement
          peer                              :: peer(),
          username                          :: undefined | username() |
                                               {preauth, string() | undefined},
          keep_alive                        :: undefined | non_neg_integer(),
          keep_alive_tref                   :: undefined | reference(),
          retry_queue=queue:new()           :: queue:queue(),
          clean_session=false               :: flag(),
          proto_ver                         :: undefined | pos_integer(),
          queue_pid                         :: pid() | undefined,

          last_time_active=os:timestamp()   :: timestamp(),
          last_trigger=os:timestamp()       :: timestamp(),

          %% config
          allow_anonymous=false             :: boolean(),
          max_client_id_size=100            :: non_neg_integer(),

          %% changeable by auth_on_register
          shared_subscription_policy=prefer_local :: shared_sub_policy(),
          max_inflight_messages=20          :: non_neg_integer(), %% 0 means unlimited
          max_message_rate=0                :: non_neg_integer(), %% 0 means unlimited
          retry_interval=20000              :: pos_integer(),
          upgrade_qos=false                 :: boolean(),
          reg_view=vmq_reg_trie             :: atom(),
          cap_settings=#cap_settings{}      :: cap_settings(),

          %% flags and settings which have a non-default value if
          %% present and default value if not present.
          def_opts                          :: map(),

          trace_fun                         :: undefined | any() %% TODO
         }).

-define(COORDINATE_REGISTRATIONS, true).
-type msg_tag() :: 'publish' | 'pubrel'.

-type state() :: #state{}.
-export_type([state/0]).
-define(state_val(Key, Args, State), prop_val(Key, Args, State#state.Key)).
-define(cap_val(Key, Args, State), prop_val(Key, Args, CAPSettings#cap_settings.Key)).

init(Peer, Opts, #mqtt_connect{keep_alive=KeepAlive,
                               proto_ver=ProtoVer} = ConnectFrame) ->
    rand:seed(exsplus, os:timestamp()),
    MountPoint = proplists:get_value(mountpoint, Opts, ""),
    SubscriberId = {string:strip(MountPoint, right, $/), undefined},
    AllowedProtocolVersions = proplists:get_value(allowed_protocol_versions,
                                                  Opts),

    PreAuthUser =
    case lists:keyfind(preauth, 1, Opts) of
        false -> undefined;
        {_, undefined} -> undefined;
        {_, PreAuth} -> {preauth, PreAuth}
    end,
    AllowAnonymous = vmq_config:get_env(allow_anonymous, false),
    SharedSubPolicy = vmq_config:get_env(shared_subscription_policy, prefer_local),
    MaxClientIdSize = vmq_config:get_env(max_client_id_size, 23),
    RetryInterval = vmq_config:get_env(retry_interval, 20),
    MaxInflightMsgs = vmq_config:get_env(max_inflight_messages, 20),
    MaxMessageSize = vmq_config:get_env(max_message_size, 0),
    MaxMessageRate = vmq_config:get_env(max_message_rate, 0),
    UpgradeQoS = vmq_config:get_env(upgrade_outgoing_qos, false),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    CAPRegister = vmq_config:get_env(allow_register_during_netsplit, false),
    CAPPublish = vmq_config:get_env(allow_publish_during_netsplit, false),
    CAPSubscribe= vmq_config:get_env(allow_subscribe_during_netsplit, false),
    CAPUnsubscribe= vmq_config:get_env(allow_unsubscribe_during_netsplit, false),
    CAPSettings = #cap_settings{
                     allow_register=CAPRegister,
                     allow_publish=CAPPublish,
                     allow_subscribe=CAPSubscribe,
                     allow_unsubscribe=CAPUnsubscribe
                    },
    TraceFun = vmq_config:get_env(trace_fun, undefined),
    DOpts0 = set_defopt(suppress_lwt_on_session_takeover, false, #{}),
    DOpts1 = set_defopt(coordinate_registrations, ?COORDINATE_REGISTRATIONS, DOpts0),

    maybe_initiate_trace(ConnectFrame, TraceFun),

    set_max_msg_size(MaxMessageSize),

    _ = vmq_metrics:incr_mqtt_connect_received(),
    %% the client is allowed "grace" of a half a time period
    set_keepalive_check_timer(KeepAlive),

    State = #state{peer=Peer,
                   upgrade_qos=UpgradeQoS,
                   subscriber_id=SubscriberId,
                   allow_anonymous=AllowAnonymous,
                   shared_subscription_policy=SharedSubPolicy,
                   max_inflight_messages=MaxInflightMsgs,
                   max_message_rate=MaxMessageRate,
                   username=PreAuthUser,
                   max_client_id_size=MaxClientIdSize,
                   keep_alive=KeepAlive,
                   keep_alive_tref=undefined,
                   retry_interval=1000 * RetryInterval,
                   cap_settings=CAPSettings,
                   reg_view=RegView,
                   def_opts = DOpts1,
                   trace_fun=TraceFun},

    case lists:member(ProtoVer, AllowedProtocolVersions) of
        true ->
            case check_connect(ConnectFrame, State) of
                {stop, Reason, Out} -> {stop, Reason, serialise([Out])};
                {NewState, Out} ->
                    {{connected, set_last_time_active(true, NewState)}, serialise([Out])};
                {NewState, Out, _SessCtrl} ->
                    {{connected, set_last_time_active(true, NewState)}, serialise([Out])}
            end;
        false ->
            lager:warning("invalid protocol version for ~p ~p",
                          [SubscriberId, ProtoVer]),
            {stop, normal, Data} = connack_terminate(?CONNACK_PROTO_VER, State),
            {stop, normal, serialise([Data])}
    end.

data_in(Data, SessionState) when is_binary(Data) ->
    data_in(Data, SessionState, []).

data_in(Data, SessionState, OutAcc) ->
    case vmq_parser:parse(Data, max_msg_size()) of
        more ->
            {ok, SessionState, Data, serialise(OutAcc)};
        {error, packet_exceeds_max_size} = E ->
            _ = vmq_metrics:incr_mqtt_error_invalid_msg_size(),
            E;
        {error, Reason} ->
            {error, Reason, serialise(OutAcc)};
        {Frame, Rest} ->
            case in(Frame, SessionState, true) of
                {stop, Reason, Out} ->
                    {stop, Reason, serialise([Out|OutAcc])};
                {NewSessionState, {throttle, MS, Out}} ->
                    {throttle, MS, NewSessionState, Rest, serialise([Out|OutAcc])};
                {NewSessionState, Out} when byte_size(Rest) == 0 ->
                    %% optimization
                    {ok, NewSessionState, Rest, serialise([Out|OutAcc])};
                {NewSessionState, Out} ->
                    data_in(Rest, NewSessionState, [Out|OutAcc])
            end
    end.

msg_in(Msg, SessionState) ->
   case in(Msg, SessionState, false) of
       {stop, Reason, Out} ->
           {stop, Reason, serialise([Out])};
       {NewSessionState, {throttle, _, Out}} ->
           %% we ignore throttling for the internal message flow
           {ok, NewSessionState, serialise([Out])};
       {NewSessionState, Out} ->
           {ok, NewSessionState, serialise([Out])}
   end.

%%% init --> | connected | --> terminate
in(Msg, {connected, State}, IsData) ->
    case connected(Msg, State) of
        {stop, _, _} = R -> R;
        {NewState, Out} ->
            {{connected, set_last_time_active(IsData, NewState)}, Out}
    end.

serialise(Frames) ->
    serialise(Frames, []).

serialise([], Acc) -> Acc;
serialise([[]|Frames], Acc) ->
    serialise(Frames, Acc);
serialise([[B|T]|Frames], Acc) when is_binary(B) ->
    serialise([T|Frames], [B|Acc]);
serialise([[F|T]|Frames], Acc) ->
    serialise([T|Frames], [vmq_parser:serialise(F)|Acc]).

maybe_initiate_trace(_Frame, undefined) ->
    ok;
maybe_initiate_trace(Frame, TraceFun) ->
    TraceFun(self(), Frame).

-spec connected(mqtt_frame(), state()) ->
    {state(), [mqtt_frame() | binary()]} |
    {state(), {throttle, duration_ms(), [mqtt_frame() | binary()]}} |
    {stop, any(), [mqtt_frame() | binary()]}.
connected(#mqtt_publish{message_id=MessageId, topic=Topic,
                        qos=QoS, retain=IsRetain,
                        payload=Payload},
          #state{subscriber_id={MountPoint,_},
                 shared_subscription_policy=SGPolicy} = State) ->
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    _ = vmq_metrics:incr_mqtt_publish_received(),
    Ret =
    case Topic of
        [<<"$SYS", _/binary>> |_] ->
            %% $SYS
            [];
        _ ->
            Msg = #vmq_msg{routing_key=Topic,
                           payload=Payload,
                           retain=unflag(IsRetain),
                           qos=QoS,
                           mountpoint=MountPoint,
                           msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                           sg_policy=SGPolicy},
            dispatch_publish(QoS, MessageId, Msg, State)
    end,
    case Ret of
        {error, not_allowed} ->
            terminate(publish_not_authorized_3_1_1, State);
        Out when is_list(Out) ->
            case do_throttle(#{}, State) of
                false ->
                    {State, Out};
                ThrottleMs when is_integer(ThrottleMs) ->
                    {State, {throttle, ThrottleMs, Out}}
            end;
        {Out, SessCtrl} when is_list(Out), is_map(SessCtrl) ->
            case do_throttle(SessCtrl, State) of
                false ->
                    {State, Out};
                ThrottleMs when is_integer(ThrottleMs) ->
                    {State, {throttle, ThrottleMs, Out}}
            end;
        {NewState, Out, SessCtrl} when is_list(Out), is_map(SessCtrl) ->
            case do_throttle(SessCtrl, State) of
                false ->
                    {NewState, Out};
                ThrottleMs when is_integer(ThrottleMs) ->
                    {NewState, {throttle, ThrottleMs, Out}}
            end
    end;
connected({mail, QPid, new_data}, #state{queue_pid=QPid} = State) ->
    vmq_queue:active(QPid),
    {State, []};
connected({mail, QPid, Msgs, _, Dropped},
          #state{subscriber_id=SubscriberId, queue_pid=QPid,
                 waiting_msgs=Waiting} = State) ->
    NewState =
    case Dropped > 0 of
        true ->
            lager:warning("subscriber ~p dropped ~p messages",
                          [SubscriberId, Dropped]),
            State;
        false ->
            State
    end,
    {NewState2, Out} =
    case handle_messages(Msgs, [], 0, NewState, Waiting) of
        {NewState1, HandledMsgs, []} ->
            vmq_queue:notify(QPid),
            {NewState1, HandledMsgs};
        {NewState1, HandledMsgs, NewWaiting} ->
            %% messages aren't delivered (yet) but are queued in this process
            %% we tell the queue to get rid of them
            vmq_queue:notify_recv(QPid),
            %% we call vmq_queue:notify as soon as
            %% the check_in_flight returns true again
            %% SEE: Comment in handle_waiting_msgs function.
            {NewState1#state{waiting_msgs=NewWaiting}, HandledMsgs}
    end,
    {NewState2, Out};
connected(#mqtt_puback{message_id=MessageId}, #state{waiting_acks=WAcks} = State) ->
    %% qos1 flow
    _ = vmq_metrics:incr_mqtt_puback_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            handle_waiting_msgs(State#state{waiting_acks=maps:remove(MessageId, WAcks)});
        not_found ->
            _ = vmq_metrics:incr_mqtt_error_invalid_puback(),
            {State, []}
    end;
connected(#mqtt_pubrec{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, retry_interval=RetryInterval,
           retry_queue=RetryQueue} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubrec_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            PubRelFrame = #mqtt_pubrel{message_id=MessageId},
            _ = vmq_metrics:incr_mqtt_pubrel_sent(),
            {State#state{
               retry_queue=set_retry(pubrel, MessageId, RetryInterval, RetryQueue),
               waiting_acks=maps:update(MessageId, PubRelFrame, WAcks)},
            [PubRelFrame]};
        #mqtt_pubrel{message_id=MessageId} = PubRelFrame ->
            %% handle PUBREC retries from the client.
            _ = vmq_metrics:incr_mqtt_pubrel_sent(),
            {State, [PubRelFrame]};
        not_found ->
            lager:debug("stopped connected session, due to unknown qos2 pubrec ~p", [MessageId]),
            _ = vmq_metrics:incr_mqtt_error_invalid_pubrec(),
            terminate(normal, State)
    end;
connected(#mqtt_pubrel{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubrel_received(),
    case maps:get({qos2, MessageId} , WAcks, not_found) of
        #mqtt_pubrec{} ->
            {NewState, Msgs} =
            handle_waiting_msgs(
              State#state{
                waiting_acks=maps:remove({qos2, MessageId}, WAcks)}),
            _ = vmq_metrics:incr_mqtt_pubcomp_sent(),
            {NewState, [#mqtt_pubcomp{message_id=MessageId}|Msgs]};
        not_found ->
            %% already delivered OR we pretended that we delivered the message
            %% as required by 3.1 . Client expects a PUBCOMP
            {State, [#mqtt_pubcomp{message_id=MessageId}]}
    end;
connected(#mqtt_pubcomp{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubcomp_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #mqtt_pubrel{} ->
            handle_waiting_msgs(State#state{
                                  waiting_acks=maps:remove(MessageId, WAcks)});
        not_found -> % error or wrong waiting_ack, definitely not well behaving client
            lager:debug("stopped connected session, due to qos2 pubrel missing ~p", [MessageId]),
            _ = vmq_metrics:incr_mqtt_error_invalid_pubcomp(),
            terminate(normal, State)
    end;
connected(#mqtt_subscribe{message_id=MessageId, topics=Topics},
          #state{proto_ver = ProtoVer} = State) ->
    #state{subscriber_id=SubscriberId, username=User,
           cap_settings=CAPSettings} = State,
    _ = vmq_metrics:incr_mqtt_subscribe_received(),
    OnAuthSuccess =
        fun(_User, _SubscriberId, MaybeChangedTopics) ->
                SubTopics = subtopics(MaybeChangedTopics, ProtoVer),
                case vmq_reg:subscribe(CAPSettings#cap_settings.allow_subscribe, SubscriberId, SubTopics) of
                    {ok, _} = Res ->
                        vmq_plugin:all(on_subscribe, [User, SubscriberId, MaybeChangedTopics]),
                        Res;
                    Res -> Res
                end
        end,
    case auth_on_subscribe(User, SubscriberId, Topics, OnAuthSuccess) of
        {ok, QoSs} ->
            Frame = #mqtt_suback{message_id=MessageId, qos_table=QoSs},
            _ = vmq_metrics:incr_mqtt_suback_sent(),
            {State, [Frame]};
        {error, not_allowed} ->
            %% allow the parser to add the 0x80 Failure return code
            QoSs = [not_allowed || _ <- Topics],
            Frame = #mqtt_suback{message_id=MessageId, qos_table=QoSs},
            _ = vmq_metrics:incr_mqtt_error_auth_subscribe(),
            {State, [Frame]};
        {error, _Reason} ->
            %% cant subscribe due to overload or netsplit,
            %% Subscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr_mqtt_error_subscribe(),
            {State, []}
    end;
connected(#mqtt_unsubscribe{message_id=MessageId, topics=Topics}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           cap_settings=CAPSettings} = State,
    _ = vmq_metrics:incr_mqtt_unsubscribe_received(),
    OnSuccess =
        fun(_SubscriberId, MaybeChangedTopics) ->
                case vmq_reg:unsubscribe(CAPSettings#cap_settings.allow_unsubscribe, SubscriberId, MaybeChangedTopics) of
                    ok ->
                        vmq_plugin:all(on_topic_unsubscribed, [SubscriberId, MaybeChangedTopics]),
                        ok;
                    V -> V
                end
        end,
    case unsubscribe(User, SubscriberId, Topics, OnSuccess) of
        ok ->
            Frame = #mqtt_unsuback{message_id=MessageId},
            _ = vmq_metrics:incr_mqtt_unsuback_sent(),
            {State, [Frame]};
        {error, _Reason} ->
            %% cant unsubscribe due to overload or netsplit,
            %% Unsubscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr_mqtt_error_unsubscribe(),
            {State, []}
    end;
connected(#mqtt_pingreq{}, State) ->
    _ = vmq_metrics:incr_mqtt_pingreq_received(),
    Frame = #mqtt_pingresp{},
    _ = vmq_metrics:incr_mqtt_pingresp_sent(),
    {State, [Frame]};
connected(#mqtt_disconnect{}, State) ->
    _ = vmq_metrics:incr_mqtt_disconnect_received(),
    terminate(mqtt_client_disconnect, State);
connected(retry,
    #state{waiting_acks=WAcks, retry_interval=RetryInterval,
           retry_queue=RetryQueue} = State) ->
    {RetryFrames, NewRetryQueue} = handle_retry(RetryInterval, RetryQueue, WAcks),
    {State#state{retry_queue=NewRetryQueue}, RetryFrames};
connected({disconnect, Reason}, State) ->
    lager:debug("stop due to disconnect", []),
    terminate(Reason, State);
connected(check_keepalive, #state{last_time_active=Last, keep_alive=KeepAlive,
                                  subscriber_id=SubscriberId, username=UserName} = State) ->
    Now = os:timestamp(),
    case timer:now_diff(Now, Last) > (1500000 * KeepAlive) of
        true ->
            lager:warning("client ~p with username ~p stopped due to keepalive expired", [SubscriberId, UserName]),
            _ = vmq_metrics:incr(?MQTT4_CLIENT_KEEPALIVE_EXPIRED),
            terminate(normal, State);
        false ->
            set_keepalive_check_timer(KeepAlive),
            {State, []}
    end;
connected({'DOWN', _MRef, process, QPid, Reason}, #state{queue_pid=QPid} = State) ->
    queue_down_terminate(Reason, State);
connected({info_req, {Ref, CallerPid}, InfoItems}, State) ->
    CallerPid ! {Ref, {ok, get_info_items(InfoItems, State)}},
    {State, []};
connected({Ref, ok}, State) when is_reference(Ref) ->
    %% Late arrival of ack after enqueueing to a remote
    %% queue.
    %%
    %% TODO: this should be cleaned up for 2.0 as changing this is
    %% likely backwards incompatible.
    {State, []};
connected({Ref, {error, cant_remote_enqueue}}, State) when is_reference(Ref) ->
    %% Late arrival of negative ack after enqueueing to a remote
    %% queue.
    %%
    %% TODO: this should be cleaned up for 2.0 as changing this is
    %% likely backwards incompatible.
    {State, []};
connected(close_timeout, State) ->
    %% Late arrival of the close_timeout that has been fired by vmq_mqtt_pre_init
    %% As we're in the connected state, it's ok to ignore this timeout
    {State, []};
connected(Unexpected, State) ->
    lager:warning("stopped connected session, due to unexpected frame type ~p", [Unexpected]),
    terminate({error, unexpected_message, Unexpected}, State).

connack_terminate(RC, _State) ->
    _ = vmq_metrics:incr({?MQTT4_CONNACK_SENT, RC}),
    {stop, normal, [#mqtt_connack{session_present=false, return_code=RC}]}.

queue_down_terminate(shutdown, State) ->
    terminate(normal, State);
queue_down_terminate(Reason, #state{queue_pid=QPid} = State) ->
    terminate({error, {queue_down, QPid, Reason}}, State).

terminate(Reason, #state{clean_session=CleanSession} = State) ->
    _ = case CleanSession of
            true -> ok;
            false ->
                handle_waiting_acks_and_msgs(State)
        end,
    %% TODO: the counter update is missing the last will message
    maybe_publish_last_will(State, Reason),
    {stop, terminate_reason(Reason), []}.

terminate_reason(publish_not_authorized_3_1_1) -> normal;
terminate_reason(Reason) ->  vmq_mqtt_fsm_util:terminate_reason(Reason).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec check_connect(mqtt_connect(), state()) ->  {state(), [mqtt_connack()], session_ctrl()} | {state(), [mqtt_connack()]} | {stop, normal, [mqtt_connack()]}.
check_connect(#mqtt_connect{proto_ver=Ver, clean_session=CleanSession} = F, State) ->
    CCleanSession = unflag(CleanSession),
    check_client_id(F, State#state{clean_session=CCleanSession, proto_ver=Ver}).

-spec check_client_id(mqtt_connect(), state()) ->  {state(), [mqtt_connack()], session_ctrl()} | {state(), [mqtt_connack()]} | {stop, normal, [mqtt_connack()]}.
check_client_id(#mqtt_connect{} = Frame,
                #state{username={preauth, UserNameFromCert}} = State) ->
    check_client_id(Frame#mqtt_connect{username=UserNameFromCert},
                    State#state{username=UserNameFromCert});

check_client_id(#mqtt_connect{client_id= <<>>, proto_ver=Ver} = F, State) when ?IS_PROTO_4(Ver) ->
    %% [MQTT-3.1.3-8]
    %% If the Client supplies a zero-byte ClientId with CleanSession set to 0,
    %% the Server MUST respond to the >CONNECT Packet with a CONNACK return
    %% code 0x02 (Identifier rejected) and then close the Network
    case State#state.clean_session of
        false ->
            connack_terminate(?CONNACK_INVALID_ID, State);
        true ->
            RandomClientId = random_client_id(),
            {MountPoint, _} = State#state.subscriber_id,
            SubscriberId = {MountPoint, RandomClientId},
            check_user(F#mqtt_connect{client_id=RandomClientId},
                       State#state{subscriber_id=SubscriberId})
    end;
check_client_id(#mqtt_connect{client_id= <<>>, proto_ver=Ver}, State) when ?IS_PROTO_3(Ver) ->
    lager:warning("empty client id not allowed in mqttv3 ~p",
                [State#state.subscriber_id]),
    connack_terminate(?CONNACK_INVALID_ID, State);
check_client_id(#mqtt_connect{client_id=ClientId} = F,
                #state{max_client_id_size=S} = State)
  when byte_size(ClientId) =< S ->
    {MountPoint, _} = State#state.subscriber_id,
    SubscriberId = {MountPoint, ClientId},
    check_user(F, State#state{subscriber_id=SubscriberId});
check_client_id(#mqtt_connect{client_id=Id}, State) ->
    lager:warning("invalid client id ~p", [Id]),
    connack_terminate(?CONNACK_INVALID_ID, State).

-spec check_user(mqtt_connect(), state()) ->  {state(), [mqtt_connack()], session_ctrl()} | {state(), [mqtt_connack()]} | {stop, normal, [mqtt_connack()]}.
check_user(#mqtt_connect{username=User, password=Password} = F, State) ->
    case State#state.allow_anonymous of
        false ->
            case auth_on_register(Password, State#state{username=User}) of
                {ok, QueueOpts, #state{peer=Peer, subscriber_id=SubscriberId, clean_session=CleanSession,
                                       cap_settings=CAPSettings, def_opts=DOpts} = State1} ->
                    CoordinateRegs = maps:get(coordinate_registrations, DOpts, ?COORDINATE_REGISTRATIONS),
                    case vmq_reg:register_subscriber(CAPSettings#cap_settings.allow_register, CoordinateRegs, SubscriberId, CleanSession, QueueOpts) of
                        {ok, #{session_present := SessionPresent,
                               initial_msg_id := MsgId,
                               queue_pid := QPid}} ->
                            monitor(process, QPid),
                            _ = vmq_plugin:all(on_register, [Peer, SubscriberId,
                                                             State1#state.username]),
                            State2 = State1#state{queue_pid=QPid, next_msg_id=MsgId},
                            check_will(F, SessionPresent, State2);
                        {error, Reason} ->
                            lager:warning("can't register client ~p with username ~p due to ~p",
                                          [SubscriberId, User, Reason]),
                            connack_terminate(?CONNACK_SERVER, State1)
                    end;
                {error, no_matching_hook_found} ->
                    lager:error("can't authenticate client ~p from ~s due to no_matching_hook_found",
                                [State#state.subscriber_id, peertoa(State#state.peer)]),
                    connack_terminate(?CONNACK_AUTH, State);
                {error, invalid_credentials} ->
                    lager:warning(
                      "can't authenticate client ~p from ~s due to invalid_credentials",
                      [State#state.subscriber_id, peertoa(State#state.peer)]),
                    connack_terminate(?CONNACK_CREDENTIALS, State);
                {error, Error} ->
                    %% can't authenticate due to other reason
                    lager:warning(
                      "can't authenticate client ~p from ~s due to ~p",
                      [State#state.subscriber_id, peertoa(State#state.peer), Error]),
                    connack_terminate(?CONNACK_AUTH, State)
            end;
        true ->
            #state{peer=Peer, subscriber_id=SubscriberId,
                   cap_settings=CAPSettings, clean_session=CleanSession,
                   def_opts=DOpts} = State,
            CoordinateRegs = maps:get(coordinate_registrations, DOpts, ?COORDINATE_REGISTRATIONS),
            case vmq_reg:register_subscriber(CAPSettings#cap_settings.allow_register, CoordinateRegs, SubscriberId, CleanSession, queue_opts(State, [])) of
                {ok, #{session_present := SessionPresent,
                       initial_msg_id := MsgId,
                       queue_pid := QPid}} ->
                    monitor(process, QPid),
                    _ = vmq_plugin:all(on_register, [Peer, SubscriberId, User]),
                    check_will(F, SessionPresent, State#state{queue_pid=QPid, username=User, next_msg_id=MsgId});
                {error, Reason} ->
                    lager:warning("can't register client ~p due to reason ~p",
                                [SubscriberId, Reason]),
                    connack_terminate(?CONNACK_SERVER, State)
            end
    end.

-spec check_will(mqtt_connect(), flag(), state()) ->  {state(), [mqtt_connack()], session_ctrl()} | {state(), [mqtt_connack()]} | {stop, normal, [mqtt_connack()]}.
check_will(#mqtt_connect{will_topic=undefined, will_msg=undefined}, SessionPresent, State) ->
    _ = vmq_metrics:incr({?MQTT4_CONNACK_SENT, ?CONNACK_ACCEPT}),
    {State, [#mqtt_connack{session_present=SessionPresent, return_code=?CONNACK_ACCEPT}]};
check_will(#mqtt_connect{will_topic=Topic, will_msg=Payload, will_qos=Qos, will_retain=IsRetain},
           SessionPresent, State) ->
    #state{username=User, subscriber_id={MountPoint, _}=SubscriberId} = State,
    case auth_on_publish(User, SubscriberId,
                         #vmq_msg{routing_key=Topic,
                                  payload=Payload,
                                  msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                                  qos=Qos,
                                  retain=unflag(IsRetain),
                                  mountpoint=MountPoint
                                 },
                         fun(Msg, _, SessCtrl) -> {ok, Msg, SessCtrl} end) of
        {ok, Msg, SessCtrl} ->
            _ = vmq_metrics:incr({?MQTT4_CONNACK_SENT, ?CONNACK_ACCEPT}),
            {State#state{will_msg=Msg},
             [#mqtt_connack{session_present=SessionPresent,
                            return_code=?CONNACK_ACCEPT}],
             SessCtrl};
        {error, Reason} ->
            lager:warning("can't authenticate last will
                          for client ~p due to ~p", [SubscriberId, Reason]),
            connack_terminate(?CONNACK_AUTH, State)
    end.

-spec auth_on_register(password(), state()) -> {error, atom()} | {ok, map(), state()}.
auth_on_register(Password, State) ->
    #state{clean_session=Clean, peer=Peer, cap_settings=CAPSettings,
           subscriber_id=SubscriberId, username=User} = State,
    HookArgs = [Peer, SubscriberId, User, Password, Clean],
    case vmq_plugin:all_till_ok(auth_on_register, HookArgs) of
        ok ->
            {ok, queue_opts(State, []), State};
        {ok, Args} ->
            set_sock_opts(prop_val(tcp_opts, Args, [])),
            ChangedCAPSettings
            = CAPSettings#cap_settings{
                allow_register=?cap_val(allow_register, Args, CAPSettings),
                allow_publish=?cap_val(allow_publish, Args, CAPSettings),
                allow_subscribe=?cap_val(allow_subscribe, Args, CAPSettings),
                allow_unsubscribe=?cap_val(allow_unsubscribe, Args, CAPSettings)
               },

            %% for efficiency reason the max_message_size isn't kept in the state
            set_max_msg_size(prop_val(max_message_size, Args, max_msg_size())),

            ChangedState = State#state{
                             subscriber_id=?state_val(subscriber_id, Args, State),
                             username=?state_val(username, Args, State),
                             clean_session=?state_val(clean_session, Args, State),
                             reg_view=?state_val(reg_view, Args, State),
                             max_message_rate=?state_val(max_message_rate, Args, State),
                             max_inflight_messages=?state_val(max_inflight_messages, Args, State),
                             shared_subscription_policy=?state_val(shared_subscription_policy, Args, State),
                             retry_interval=?state_val(retry_interval, Args, State),
                             upgrade_qos=?state_val(upgrade_qos, Args, State),
                             cap_settings=ChangedCAPSettings
                            },
            {ok, queue_opts(ChangedState, Args), ChangedState};
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_sock_opts([any()]) -> {'set_sock_opts',_}.
set_sock_opts(Opts) ->
    self() ! {set_sock_opts, Opts}.

-spec auth_on_subscribe(username(), subscriber_id(), [{topic(), qos()}],
                        fun((username(), subscriber_id(), [{topic(), qos()}]) ->
                                   {ok, [qos() | not_allowed]} | {error, atom()})
                       ) -> {ok, [qos() | not_allowed]} | {error, atom()}.
auth_on_subscribe(User, SubscriberId, Topics, AuthSuccess) ->
    case vmq_plugin:all_till_ok(auth_on_subscribe,
                                [User, SubscriberId, Topics]) of
        ok ->
            AuthSuccess(User, SubscriberId, Topics);
        {ok, NewTopics} when is_list(NewTopics) ->
            AuthSuccess(User, SubscriberId, NewTopics);
        {error, _} ->
            {error, not_allowed}
    end.

-spec unsubscribe(username(), subscriber_id(), [{topic(), qos()}],
                  fun((subscriber_id(), [{topic(), qos()}]) ->
                             ok | {error, not_ready})
                 ) -> ok | {error, not_ready}.
unsubscribe(User, SubscriberId, Topics, UnsubFun) ->
    TTopics =
        case vmq_plugin:all_till_ok(on_unsubscribe, [User, SubscriberId, Topics]) of
            ok ->
                Topics;
            {ok, [[W|_]|_] = NewTopics} when is_binary(W) ->
                NewTopics;
            {error, _} ->
                Topics
        end,
    UnsubFun(SubscriberId, TTopics).

-spec auth_on_publish(username(), subscriber_id(), msg(), aop_success_fun()) ->
                             {ok, msg(), session_ctrl()} |
                             {error, atom()}.
auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=Topic,
                                             payload=Payload,
                                             qos=QoS,
                                             retain=IsRetain} = Msg,
                AuthSuccess) ->
    HookArgs = [User, SubscriberId, QoS, Topic, Payload, unflag(IsRetain)],
    case vmq_plugin:all_till_ok(auth_on_publish, HookArgs) of
        ok ->
            AuthSuccess(Msg, HookArgs, #{});
        {ok, ChangedPayload} when is_binary(ChangedPayload) ->
            HookArgs1 = [User, SubscriberId, QoS, Topic, ChangedPayload, unflag(IsRetain)],
            AuthSuccess(Msg#vmq_msg{payload=ChangedPayload}, HookArgs1, #{});
        {ok, Args} when is_list(Args) ->
            #vmq_msg{mountpoint=MP} = Msg,
            ChangedTopic = proplists:get_value(topic, Args, Topic),
            ChangedPayload = proplists:get_value(payload, Args, Payload),
            ChangedQoS = proplists:get_value(qos, Args, QoS),
            ChangedIsRetain = proplists:get_value(retain, Args, IsRetain),
            ChangedMountpoint = proplists:get_value(mountpoint, Args, MP),
            HookArgs1 = [User, SubscriberId, ChangedQoS,
                         ChangedTopic, ChangedPayload, ChangedIsRetain],
            SessCtrl = session_ctrl(Args),
            AuthSuccess(Msg#vmq_msg{routing_key=ChangedTopic,
                                    payload=ChangedPayload,
                                    qos=ChangedQoS,
                                    retain=ChangedIsRetain,
                                    mountpoint=ChangedMountpoint},
                        HookArgs1,
                        SessCtrl);
        {error, Re} ->
            lager:error("can't auth publish ~p due to ~p", [HookArgs, Re]),
            {error, not_allowed}
    end.

-spec session_ctrl([any()]) -> #{'throttle'=>pos_integer()}.
session_ctrl(Args) ->
    case lists:keyfind(throttle, 1, Args) of
        false -> #{};
        {throttle, ThrottleMs} when is_integer(ThrottleMs),
                                    ThrottleMs > 0 ->
            #{throttle => ThrottleMs}
    end.

-spec publish(cap_settings(), module(), username(), subscriber_id(), msg()) ->
                     {ok, msg(), session_ctrl()} |
                     {error, atom()}.
publish(CAPSettings, RegView, User, {_, ClientId}=SubscriberId, Msg) ->
    auth_on_publish(User, SubscriberId, Msg,
                    fun(MaybeChangedMsg, HookArgs, SessCtrl) ->
                            case on_publish_hook(vmq_reg:publish(CAPSettings#cap_settings.allow_publish, RegView, ClientId, MaybeChangedMsg),
                                            HookArgs) of
                                ok ->
                                    {ok, MaybeChangedMsg, SessCtrl};
                                E -> E
                            end
                    end).

-spec on_publish_hook({ok, {integer(), integer()}} | {error, _}, list()) -> ok | {error, _}.
on_publish_hook({ok, _NumMatched}, HookParams) ->
    _ = vmq_plugin:all(on_publish, HookParams),
    ok;
on_publish_hook(Other, _) -> Other.

-spec dispatch_publish(qos(), msg_id(), msg(), state()) ->
    list() |
    {list(), session_ctrl()} |
    {state(), list(), session_ctrl()} |
    {error, not_allowed}.
dispatch_publish(Qos, MessageId, Msg, State) ->
    dispatch_publish_(Qos, MessageId, Msg, State).

-spec dispatch_publish_(qos(), msg_id(), msg(), state()) -> 
    list()
    | {list(), session_ctrl()}
    | {state(), list(), session_ctrl()} 
    | {error, not_allowed}.
dispatch_publish_(0, MessageId, Msg, State) ->
    dispatch_publish_qos0(MessageId, Msg, State);
dispatch_publish_(1, MessageId, Msg, State) ->
    dispatch_publish_qos1(MessageId, Msg, State);
dispatch_publish_(2, MessageId, Msg, State) ->
    dispatch_publish_qos2(MessageId, Msg, State).

-spec dispatch_publish_qos0(msg_id(), msg(), state()) ->
    list()
    | {list(), session_ctrl()}
    | {error, not_allowed}.
dispatch_publish_qos0(_MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto,
           cap_settings=CAPSettings, reg_view=RegView} = State,
    case publish(CAPSettings, RegView, User, SubscriberId, Msg) of
        {ok, _, SessCtrl} ->
            {[], SessCtrl};
        {error, not_allowed} when ?IS_PROTO_4(Proto) ->
            %% we have to close connection for 3.1.1
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            {error, not_allowed};
        {error, _Reason} ->
            %% can't publish due to overload or netsplit
            _ = vmq_metrics:incr_mqtt_error_publish(),
            []
    end.

-spec dispatch_publish_qos1(msg_id(), msg(), state()) ->
    list()
    | {list(), session_ctrl()}
    | {error, not_allowed}.
dispatch_publish_qos1(MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto,
           cap_settings=CAPSettings, reg_view=RegView} = State,
    case publish(CAPSettings, RegView, User, SubscriberId, Msg) of
        {ok, _, SessCtrl} ->
            _ = vmq_metrics:incr_mqtt_puback_sent(),
            {[#mqtt_puback{message_id=MessageId}], SessCtrl};
        {error, not_allowed} when ?IS_PROTO_4(Proto) ->
            %% we have to close connection for 3.1.1
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            {error, not_allowed};
        {error, not_allowed} ->
            %% we pretend as everything is ok for 3.1 and Bridge
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            _ = vmq_metrics:incr_mqtt_puback_sent(),
            [#mqtt_puback{message_id=MessageId}];
        {error, _Reason} ->
            %% can't publish due to overload or netsplit
            _ = vmq_metrics:incr_mqtt_error_publish(),
            []
    end.

-spec dispatch_publish_qos2(msg_id(), msg(), state()) ->
    list()
    | {state(), list(), session_ctrl()}
    | {error, not_allowed}.
dispatch_publish_qos2(MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto,
           cap_settings=CAPSettings, reg_view=RegView, waiting_acks=WAcks} = State,
    case maps:get({qos2, MessageId}, WAcks, not_found) of
        not_found ->
            case publish(CAPSettings, RegView, User, SubscriberId, Msg) of
                {ok, _, SessCtrl} ->
                    Frame = #mqtt_pubrec{message_id=MessageId},
                    _ = vmq_metrics:incr_mqtt_pubrec_sent(),
                    {State#state{
                       waiting_acks=maps:put({qos2, MessageId}, Frame, WAcks)},
                     [Frame], SessCtrl};
                {error, not_allowed} when ?IS_PROTO_4(Proto) ->
                    %% we have to close connection for 3.1.1
                    _ = vmq_metrics:incr_mqtt_error_auth_publish(),
                    {error, not_allowed};
                {error, not_allowed} ->
                    %% we pretend as everything is ok for 3.1 and Bridge
                    _ = vmq_metrics:incr_mqtt_error_auth_publish(),
                    _ = vmq_metrics:incr_mqtt_pubrec_sent(),
                Frame = #mqtt_pubrec{message_id=MessageId},
                    [Frame];
                {error, _Reason} ->
                    %% can't publish due to overload or netsplit
                    _ = vmq_metrics:incr_mqtt_error_publish(),
                    []
            end;
        _Frame ->
            Frame = #mqtt_pubrec{message_id=MessageId},
            [Frame]
    end.

-spec handle_waiting_acks_and_msgs(state()) -> ok.
handle_waiting_acks_and_msgs(State) ->
    #state{waiting_acks=WAcks, waiting_msgs=WMsgs, queue_pid=QPid, next_msg_id=NextMsgId} = State,
    MsgsToBeDeliveredNextTime =
    lists:foldl(fun ({{qos2, _}, _}, Acc) ->
                      Acc;
                  ({MsgId, #mqtt_pubrel{} = Frame}, Acc) ->
                      %% unacked PUBREL Frame
                      [{deliver_pubrel, {MsgId, Frame}}|Acc];
                  ({MsgId, #vmq_msg{qos=QoS} = Msg}, Acc) ->
                      [#deliver{qos=QoS, msg_id=MsgId, msg=Msg#vmq_msg{dup=true}}|Acc]
              end, lists:reverse(WMsgs),
                %% 3. the sorted list has the oldest waiting-ack at the head.
                %% the way we add it to the accumulator 'WMsgs' we have to
                %% reverse the list to make sure the oldest stays at the head.
                lists:reverse(
                  %% 2. we have to sort this list given the message id
                  lists:keysort(1,
                                %% 1. maps:to_list gives us an
                                %% arbitrary ordered list, (it looks
                                %% like it is ordered with the newest
                                %% item at the top)
                                maps:to_list(WAcks)
                               )
                 )),
    catch vmq_queue:set_last_waiting_acks(QPid, MsgsToBeDeliveredNextTime, NextMsgId).

-spec handle_waiting_msgs(state()) -> {state(), [msg_id()]}.
handle_waiting_msgs(#state{waiting_msgs=[]} = State) ->
    {State, []};
handle_waiting_msgs(#state{waiting_msgs=Msgs, queue_pid=QPid} = State) ->
    case handle_messages(lists:reverse(Msgs), [], 0, State, []) of
        {NewState, HandledMsgs, []} ->
            %% we're ready to take more
            vmq_queue:notify(QPid),
            {NewState#state{waiting_msgs=[]}, HandledMsgs};
        {NewState, HandledMsgs, Waiting} ->
            %% TODO: since we don't notfiy the queue it is now possible
            %% that ALSO QoS0 messages are getting queued up, and need
            %% to wait until check_in_flight(_) returns true again.
            %% That's unfortunate, but would need a different implementation
            %% of the vmq_queue FSM which differentiates between QoS.
            {NewState#state{waiting_msgs=Waiting}, HandledMsgs}
    end.

handle_messages([#deliver{qos=0}=D|Rest], Frames, PubCnt, State, Waiting) ->
    {Frame, NewState} = prepare_frame(D, State),
    handle_messages(Rest, [Frame|Frames], PubCnt + 1, NewState, Waiting);
handle_messages([#deliver{} = Obj|Rest], Frames, PubCnt, State, Waiting) ->
    case check_in_flight(State) of
        true ->
            {Frame, NewState} = prepare_frame(Obj, State),
            handle_messages(Rest, [Frame|Frames], PubCnt + 1,  NewState, Waiting);
        false ->
            % only qos 1&2 are constrained by max_in_flight
            handle_messages(Rest, Frames, PubCnt, State, [Obj|Waiting])
    end;
handle_messages([{deliver_pubrel, {MsgId, #mqtt_pubrel{} = Frame}}|Rest], Frames, PubCnt, State0, Waiting) ->
    %% this is called when a pubrel is retried after a client reconnects
    #state{waiting_acks=WAcks, retry_interval=RetryInterval, retry_queue=RetryQueue} = State0,
    _ = vmq_metrics:incr_mqtt_pubrel_sent(),
    State1 = State0#state{retry_queue=set_retry(pubrel, MsgId, RetryInterval, RetryQueue),
                          waiting_acks=maps:put(MsgId, Frame, WAcks)},
    handle_messages(Rest, [Frame|Frames], PubCnt, State1, Waiting);
handle_messages([], [], _, State, Waiting) ->
    {State, [], Waiting};
handle_messages([], Frames, PubCnt, State, Waiting) ->
    _ = vmq_metrics:incr_mqtt_publishes_sent(PubCnt),
    {State, Frames, Waiting}.

prepare_frame(#deliver{qos=QoS, msg_id=MsgId, msg=Msg}, State) ->
    #state{username=User, subscriber_id=SubscriberId, waiting_acks=WAcks,
           retry_queue=RetryQueue, retry_interval=RetryInterval} = State,
    #vmq_msg{routing_key=Topic,
             payload=Payload,
             retain=IsRetained,
             dup=IsDup,
             qos=MsgQoS} = Msg,
    NewQoS = maybe_upgrade_qos(QoS, MsgQoS, State),
    {NewTopic, NewPayload} =
    case on_deliver_hook(User, SubscriberId, QoS, Topic, Payload, IsRetained) of
        {error, _} ->
            %% no on_deliver hook specified... that's ok
            {Topic, Payload};
        ok ->
            {Topic, Payload};
        {ok, ChangedPayload} when is_binary(ChangedPayload) ->
            {Topic, ChangedPayload};
        {ok, Args} when is_list(Args) ->
            ChangedTopic = proplists:get_value(topic, Args, Topic),
            ChangedPayload = proplists:get_value(payload, Args, Payload),
            {ChangedTopic, ChangedPayload}
    end,
    {OutgoingMsgId, State1} = get_msg_id(NewQoS, MsgId, State),
    Frame = #mqtt_publish{message_id=OutgoingMsgId,
                          topic=NewTopic,
                          qos=NewQoS,
                          retain=IsRetained,
                          dup=IsDup,
                          payload=NewPayload},
    case NewQoS of
        0 ->
            {Frame, State1};
        _ ->
            {Frame, State1#state{
                      retry_queue=set_retry(publish, OutgoingMsgId, RetryInterval, RetryQueue),
                      waiting_acks=maps:put(OutgoingMsgId,
                                            Msg#vmq_msg{qos=NewQoS}, WAcks)}}
    end.

-spec on_deliver_hook(username(), subscriber_id(), qos(), topic(), payload(), flag()) -> any().
on_deliver_hook(User, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    HookArgs0 = [User, SubscriberId, Topic, Payload],
    case vmq_plugin:all_till_ok(on_deliver, HookArgs0) of
        {error, _} ->
            HookArgs1 = [User, SubscriberId, QoS, Topic, Payload, IsRetain],
            vmq_plugin:all_till_ok(on_deliver, HookArgs1);
        Other -> Other
    end.

-spec maybe_publish_last_will(state(), any()) -> ok.
maybe_publish_last_will(_, ?CLIENT_DISCONNECT) -> ok;
maybe_publish_last_will(#state{will_msg=undefined}, _) -> ok;
maybe_publish_last_will(#state{subscriber_id={_, ClientId}=SubscriberId, username=User,
                               will_msg=Msg, reg_view=RegView, cap_settings=CAPSettings,
                               def_opts=DOpts},
                        Reason) ->
    case suppress_lwt(Reason, DOpts) of
        false ->
            #vmq_msg{qos=QoS, routing_key=Topic, payload=Payload, retain=IsRetain} = Msg,
            HookArgs = [User, SubscriberId, QoS, Topic, Payload, IsRetain],
            _ = on_publish_hook(vmq_reg:publish(CAPSettings#cap_settings.allow_publish,
                                                RegView, ClientId, Msg), HookArgs),
            ok;
        true ->
            lager:debug("last will and testament suppressed on session takeover for subscriber ~p",
                        [SubscriberId]),
            ok
    end.

-spec suppress_lwt(_,map()) -> boolean().
suppress_lwt(?SESSION_TAKEN_OVER, #{suppress_lwt_on_session_takeover := true}) ->
    true;
suppress_lwt(_Reason, _DOpts) ->
    false.

-spec check_in_flight(state()) -> boolean().
check_in_flight(#state{waiting_acks=WAcks, max_inflight_messages=Max}) ->
    case Max of
        0 -> true;
        V ->
            maps:size(WAcks) < V
    end.

%% The MQTT specification requires that the QoS of a message delivered to a
%% subscriber is never upgraded to match the QoS of the subscription. If
%% upgrade_outgoing_qos is set true, messages sent to a subscriber will always
%% match the QoS of its subscription. This is a non-standard option not provided
%% for by the spec.
-spec maybe_upgrade_qos(qos(), qos(),state()) -> qos().
maybe_upgrade_qos(SubQoS, PubQoS, _) when SubQoS =< PubQoS ->
    %% already ref counted in vmq_reg
    SubQoS;
maybe_upgrade_qos(SubQoS, PubQoS, #state{upgrade_qos=true}) when SubQoS > PubQoS ->
    %% already ref counted in vmq_reg
    SubQoS;
maybe_upgrade_qos(_, PubQoS, _) ->
    %% matches when PubQoS is smaller than SubQoS and upgrade_qos=false
    %% SubQoS = 0, PubQoS cannot be smaller than 0, --> matched in first case
    %% SubQoS = 1|2, PubQoS = 0 ---> this case
    PubQoS.

-spec get_msg_id(qos(), undefined | msg_id(), state()) -> {msg_id(), state()}.
get_msg_id(0, _, State) ->
    {undefined, State};
get_msg_id(_, MsgId, State) when is_integer(MsgId) ->
    {MsgId, State};
get_msg_id(_, undefined, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, undefined, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

-spec random_client_id() -> binary().
random_client_id() ->
    list_to_binary(["anon-", base64:encode_to_string(crypto:strong_rand_bytes(20))]).

-spec set_keepalive_check_timer(non_neg_integer()) -> 'ok'.
set_keepalive_check_timer(0) -> ok;
set_keepalive_check_timer(KeepAlive) ->
    %% This allows us to heavily reduce start and cancel timers,
    %% however we're losing precision. But that's ok for the keepalive timer.
    _ = vmq_mqtt_fsm_util:send_after(KeepAlive * 750, check_keepalive),
    ok.


-spec do_throttle(session_ctrl(), state()) -> false | duration_ms().
do_throttle(#{throttle := ThrottleMs}, _) -> ThrottleMs;
do_throttle(_, #state{max_message_rate=0}) -> false;
do_throttle(_, #state{max_message_rate=Rate}) ->
    case vmq_metrics:check_rate(msg_in_rate, Rate) of
        false -> 1000;
        _ -> false
    end.

-spec set_last_time_active(boolean(), state()) -> state().
set_last_time_active(true, State) ->
    Now = os:timestamp(),
    State#state{last_time_active=Now};
set_last_time_active(false, State) ->
    State.

%% Retry Mechanism:
%%
%% Erlang's timer stick to a millisecond resolution, this can be problematic
%% if we have to retry many frames at the same time. This can happen during
%% heavy load, but also if a reconnect triggers a buch of offline messages
%% delivered to the client. If the client doesn't ack those messages we'll
%% retry many messages within the same millisecond. The firing of the timers
%% within the same millisecond aren't properly ordered. This can be simply
%% reproduced via the shell:
%%
%%  [erlang:send_after(1000, self(), I) || I <- lists:seq(1, 50)].
%%  ... after 1 second
%%  flush().
%%
%%  the result shows the unordered output of the fired timers.
%%
%%  To overcome this problem we stick to an own timer queue instead of setting
%%  a new timer for every frame to be retried. This enables to keep at most
%%  one retry timer per session at any time.
%%
%%  Timer Queue:
%%  When the retry timer fires `handle_retry/3` takes the oldest item from the
%%  queue and checks if a retry is required (no ack received on time). If it is
%%  required it adds the frame to the outgoing accumulator. If the frame has
%%  already been acked it ignores it. This process is repeated until the queue
%%  is empty or the time difference between timestamp a popped queue item  is
%%  smaller than the retry interval. In this case we set a new retry timer
%%  wrt. to the time difference.
%%
-spec set_retry(msg_tag(),pos_integer(),pos_integer(),queue:queue(_)) -> queue:queue(_).
set_retry(MsgTag, MsgId, Interval, RetryQueue) ->
    case queue:is_empty(RetryQueue) of
        true ->
            %% no waiting ack
            vmq_mqtt_fsm_util:send_after(Interval, retry),
            Now = os:timestamp(),
            queue:in({Now, {MsgTag, MsgId}}, RetryQueue);
        false ->
            Now = os:timestamp(),
            queue:in({Now, {MsgTag, MsgId}}, RetryQueue)
    end.

handle_retry(Interval, RetryQueue, WAcks) ->
    %% the fired timer was set for the oldest element in the queue!
    Now = os:timestamp(),
    handle_retry(Now, Interval, queue:out(RetryQueue), WAcks, []).

handle_retry(Now, Interval, {{value, {Ts, {MsgTag, MsgId} = RetryId } = Val}, RetryQueue}, WAcks, Acc) ->
    NowDiff = timer:now_diff(Now, Ts) div 1000,
    case NowDiff < Interval of
        true ->
            vmq_mqtt_fsm_util:send_after(Interval - NowDiff, retry),
            {Acc, queue:in_r(Val, RetryQueue)};
        false ->
            case get_retry_frame(MsgTag, MsgId, maps:get(MsgId, WAcks, not_found), Acc) of
                already_acked ->
                    handle_retry(Now, Interval, queue:out(RetryQueue), WAcks, Acc);
                NewAcc ->
                    NewRetryQueue = queue:in({Now, RetryId}, RetryQueue),
                    handle_retry(Now, Interval, queue:out(NewRetryQueue), WAcks, NewAcc)
            end
    end;
handle_retry(_, Interval, {empty, Queue}, _, Acc) when length(Acc) > 0 ->
    vmq_mqtt_fsm_util:send_after(Interval, retry),
    {Acc, Queue};
handle_retry(_, _, {empty, Queue}, _, Acc) ->
    {Acc, Queue}.

-spec get_retry_frame(_,msg_id(),msg(), list()) -> 'already_acked' | [any()].
get_retry_frame(publish, MsgId, #vmq_msg{routing_key=Topic, qos=QoS, retain=Retain,
                         payload=Payload}, Acc) ->
    _ = vmq_metrics:incr_mqtt_publish_sent(),
    Frame = #mqtt_publish{message_id=MsgId,
                          topic=Topic,
                          qos=QoS,
                          retain=Retain,
                          dup=true,
                          payload=Payload},
    [Frame|Acc];
get_retry_frame(pubrel, _MsgId, #mqtt_pubrel{} = Frame, Acc) ->
    _ = vmq_metrics:incr_mqtt_pubrel_sent(),
    [Frame|Acc];
get_retry_frame(_, _, _, _) ->
    %% already acked
    already_acked.

-spec prop_val(atom(),[any()],atom() | binary() | maybe_improper_list() | integer() | tuple()) -> any().
prop_val(Key, Args, Default) when is_tuple(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_tuple/1);
prop_val(Key, Args, Default) when is_list(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_list/1);
prop_val(Key, Args, Default) when is_integer(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_integer/1);
prop_val(Key, Args, Default) when is_boolean(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_boolean/1);
prop_val(Key, Args, Default) when is_atom(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_atom/1);
prop_val(Key, Args, Default) when is_binary(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_binary/1).

-spec prop_val(atom(),[any()],atom() | binary() | maybe_improper_list() | integer() | tuple(),fun((_) -> any())) -> any().
prop_val(Key, Args, Default, Validator) ->
    case proplists:get_value(Key, Args) of
        undefined -> Default;
        Val -> case Validator(Val) of
                   true -> Val;
                   false -> Default
               end
    end.
    
-spec queue_opts(state(), [any()]) -> map().
queue_opts(#state{clean_session=CleanSession}, Args) ->
    Opts = maps:from_list([{cleanup_on_disconnect, CleanSession}| Args]),
    maps:merge(vmq_queue:default_opts(), Opts).

-spec unflag(boolean() | 0 | 1) -> boolean().
unflag(true) -> true;
unflag(false) -> false;
unflag(?true) -> true;
unflag(?false) -> false.

-spec max_msg_size() -> non_neg_integer().
max_msg_size() ->
    case get(max_msg_size) of
        undefined ->
            %% no limit
            0;
        V -> V
    end.

-spec set_max_msg_size(non_neg_integer()) -> non_neg_integer().
set_max_msg_size(MaxMsgSize) when MaxMsgSize >= 0 ->
    put(max_msg_size, MaxMsgSize),
    MaxMsgSize.

-spec info(pid(),_) -> any(). % what are Items here? related to info items?
info(Pid, Items) ->
    Ref = make_ref(),
    CallerRef = {Ref, self()},
    MRef = monitor(process, Pid),
    vmq_mqtt_fsm_util:send(Pid, {info_req, CallerRef, Items}),
    receive
        {Ref, Ret} -> Ret;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

get_info_items([], State) ->
    DefaultItems = [pid, client_id, user, peer_host, peer_port],
    get_info_items(DefaultItems, State);
get_info_items(Items, State) ->
    get_info_items(Items, State, []).

-spec get_info_items([any()],state(),[{'client_id' | 'mountpoint' | 'node' | 'peer_host' | 'peer_port' | 'pid' | 'protocol' | 'timeout' | 'user' | 'waiting_acks',atom() | 
    binary() | pid() | [any()] | non_neg_integer()}]) -> 
            [{'client_id' | 'mountpoint' | 'node' | 'peer_host' | 'peer_port' | 'pid' | 'protocol' | 'timeout' | 'user' | 'waiting_acks',
            atom() | binary() | pid() | [any()] | non_neg_integer()}].
get_info_items([pid|Rest], State, Acc) ->
    get_info_items(Rest, State, [{pid, self()}|Acc]);
get_info_items([client_id|Rest], State, Acc) ->
    #state{subscriber_id={_, ClientId}} = State,
    get_info_items(Rest, State, [{client_id, ClientId}|Acc]);
get_info_items([mountpoint|Rest], State, Acc) ->
    #state{subscriber_id={MountPoint, _}} = State,
    get_info_items(Rest, State, [{mountpoint, MountPoint}|Acc]);
get_info_items([user|Rest], State, Acc) ->
    User =
    case State#state.username of
        {preauth, UserName} -> UserName;
        UserName -> UserName
    end,
    get_info_items(Rest, State, [{user, User}|Acc]);
get_info_items([node|Rest], State, Acc) ->
    get_info_items(Rest, State, [{node, node()}|Acc]);
get_info_items([peer_port|Rest], State, Acc) ->
    {_PeerIp, PeerPort} = State#state.peer,
    get_info_items(Rest, State, [{peer_port, PeerPort}|Acc]);
get_info_items([peer_host|Rest], State, Acc) ->
    {PeerIp, _} = State#state.peer,
    Host = list_to_binary(inet:ntoa(PeerIp)),
    get_info_items(Rest, State, [{peer_host, Host}|Acc]);
get_info_items([protocol|Rest], State, Acc) ->
    get_info_items(Rest, State, [{protocol, State#state.proto_ver}|Acc]);
get_info_items([timeout|Rest], State, Acc) ->
    get_info_items(Rest, State,
                   [{timeout, State#state.keep_alive}|Acc]);
get_info_items([retry_timeout|Rest], State, Acc) ->
    get_info_items(Rest, State,
                   [{timeout, State#state.retry_interval}|Acc]);
get_info_items([waiting_acks|Rest], State, Acc) ->
    Size = maps:size(State#state.waiting_acks),
    get_info_items(Rest, State,
                   [{waiting_acks, Size}|Acc]);
get_info_items([_|Rest], State, Acc) ->
    get_info_items(Rest, State, Acc);
get_info_items([], _, Acc) -> Acc.

-spec set_defopt('coordinate_registrations' | 'suppress_lwt_on_session_takeover', boolean(), map()) -> map().
set_defopt(Key, Default, Map) ->
    case vmq_config:get_env(Key, Default) of
        Default ->
            Map;
        NonDefault ->
            maps:put(Key, NonDefault, Map)
    end.

-spec peertoa(peer()) -> string().
peertoa({_IP, _Port} = Peer) ->
    vmq_mqtt_fsm_util:peertoa(Peer).

-spec subtopics([{topic(),0 | 1 | 2} | 
#mqtt5_subscribe_topic{topic::topic(),
qos::0 | 1 | 2,no_local::'empty' | 'false' | 'true' | 0 | 1,
rap::'empty' | 'false' | 'true' | 0 | 1,
retain_handling::'dont_send' | 'send_if_new_sub' | 'send_retain'}],'undefined' | pos_integer()) -> [{_,0 | 1 | 2 | {_,_}}].
subtopics(Topics, ProtoVer) when ?IS_BRIDGE(ProtoVer) ->
    %% bridge connection
    SubTopics = vmq_mqtt_fsm_util:to_vmq_subtopics(Topics, undefined),
    lists:map(fun({T, QoS}) -> {T, {QoS, #{rap => true,
                                           no_local => true}}} end, SubTopics);
subtopics(Topics, _Proto) ->
    vmq_mqtt_fsm_util:to_vmq_subtopics(Topics, undefined).
