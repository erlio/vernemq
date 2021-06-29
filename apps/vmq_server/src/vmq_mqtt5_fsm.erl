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

-module(vmq_mqtt5_fsm).
-include_lib("vmq_commons/include/vmq_types.hrl").
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include("vmq_server.hrl").
-include("vmq_metrics.hrl").

-export([init/3,
         data_in/2,
         msg_in/2,
         info/2]).

-export([msg_ref/0]).

-define(FC_RECEIVE_MAX, 16#FFFF).
-define(EXPIRY_INT_MAX, 16#FFFFFFFF).
-define(MAX_PACKET_SIZE, 16#FFFFFFF).

-type timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-record(cap_settings, {
         allow_register = false    :: boolean(),
         allow_publish = false     :: boolean(),
         allow_subscribe = false   :: boolean(),
         allow_unsubscribe = false   :: boolean()
         }).
-type cap_settings() :: #cap_settings{}.

-type topic_aliases_in() :: map().
-type topic_aliases_out() :: map().

-record(auth_data, {
          method :: binary(),
          data   :: any()
         }).

-type auth_data() :: auth_data().

-type receive_max() :: 1..?FC_RECEIVE_MAX.

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
          clean_start=false                 :: flag(),
          session_expiry_interval=0         :: 0..?EXPIRY_INT_MAX,
          proto_ver                         :: undefined | pos_integer(),
          queue_pid                         :: pid() | undefined,

          last_time_active=os:timestamp()   :: timestamp(),
          last_trigger=os:timestamp()       :: timestamp(),

          %% Data used for enhanced authentication and
          %% re-authentication. TODOv5: move this to pdict?
          enhanced_auth                     :: undefined | auth_data(),

          %% config
          allow_anonymous=false             :: boolean(),
          max_client_id_size=100            :: non_neg_integer(),

          %% changeable by auth_on_register
          shared_subscription_policy=prefer_local :: shared_sub_policy(),
          max_message_rate=0                :: non_neg_integer(), %% 0 means unlimited
          upgrade_qos=false                 :: boolean(),
          reg_view=vmq_reg_trie             :: atom(),
          cap_settings=#cap_settings{}      :: cap_settings(),
          topic_alias_max                   :: non_neg_integer(), %% 0 means no topic aliases allowed.
          topic_alias_max_out               :: non_neg_integer(), %% 0 means no topic aliases allowed.
          topic_aliases_in=#{}              :: topic_aliases_in(), %% topic aliases used from client to broker.
          topic_aliases_out=#{}             :: topic_aliases_out(), %% topic aliases used from broker to client.

          %% flow control
          fc_receive_max_client=?FC_RECEIVE_MAX :: receive_max(),
          fc_receive_max_broker=?FC_RECEIVE_MAX :: receive_max(),
          fc_receive_cnt=0                      :: 0 | receive_max(),
          fc_send_cnt=0                         :: 0 | receive_max(),

          %% flags and settings which have a non-default value if
          %% present and default value if not present.
          def_opts                          :: map(),

          trace_fun                        :: undefined | any() %% TODO
         }).

-define(COORDINATE_REGISTRATIONS, true).

-type state() :: #state{}.
-export_type([state/0]).
-define(state_val(Key, Args, State), prop_val(Key, Args, State#state.Key)).
-define(cap_val(Key, Args, State), prop_val(Key, Args, CAPSettings#cap_settings.Key)).

init(Peer, Opts, #mqtt5_connect{keep_alive=KeepAlive, properties=Properties,
                                proto_ver = ProtoVer} = ConnectFrame) ->
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
    MaxIncomingMessageSize = vmq_config:get_env(max_message_size, 0),
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
    TopicAliasMax = vmq_config:get_env(topic_alias_max_client, 0),
    TopicAliasMaxOut = maybe_get_topic_alias_max(Properties, vmq_config:get_env(topic_alias_max_broker, 0)),
    TraceFun = vmq_config:get_env(trace_fun, undefined),
    DOpts0 = set_defopt(suppress_lwt_on_session_takeover, false, #{}),
    DOpts1 = set_defopt(coordinate_registrations, ?COORDINATE_REGISTRATIONS, DOpts0),
    maybe_initiate_trace(ConnectFrame, TraceFun),
    set_max_incoming_msg_size(MaxIncomingMessageSize),

    _ = vmq_metrics:incr(?MQTT5_CONNECT_RECEIVED),
    %% the client is allowed "grace" of a half a time period
    set_keepalive_check_timer(KeepAlive),

    %% Flow Control
    FcReceiveMaxClient = maybe_get_receive_maximum(
                           Properties,
                           vmq_config:get_env(receive_max_client, ?FC_RECEIVE_MAX)),
    FcReceiveMaxBroker = vmq_config:get_env(receive_max_broker, ?FC_RECEIVE_MAX),

    MaxOutgoingMsgSize = maybe_get_maximum_packet_size(
                           Properties,
                           vmq_config:get_env(m5_max_packet_size, undefined)),
    set_max_outgoing_msg_size(MaxOutgoingMsgSize),

    RequestProblemInformation = maybe_get_request_problem_information(
                                  Properties,
                                  vmq_config:get_env(m5_request_problem_information, true)),
    set_request_problem_information(RequestProblemInformation),

    State = #state{peer=Peer,
                   upgrade_qos=UpgradeQoS,
                   subscriber_id=SubscriberId,
                   allow_anonymous=AllowAnonymous,
                   shared_subscription_policy=SharedSubPolicy,
                   max_message_rate=MaxMessageRate,
                   username=PreAuthUser,
                   max_client_id_size=MaxClientIdSize,
                   keep_alive=KeepAlive,
                   keep_alive_tref=undefined,
                   cap_settings=CAPSettings,
                   topic_alias_max=TopicAliasMax,
                   topic_alias_max_out=TopicAliasMaxOut,
                   reg_view=RegView,
                   def_opts = DOpts1,
                   trace_fun=TraceFun,
                   fc_receive_max_client=FcReceiveMaxClient,
                   fc_receive_max_broker=FcReceiveMaxBroker,
                   last_time_active=os:timestamp()},

    case lists:member(ProtoVer, AllowedProtocolVersions) of
        true ->
            case check_enhanced_auth(ConnectFrame, State) of
                {stop, _, _} = R -> R;
                {pre_connect_auth, NewState, Out} ->
                    {{pre_connect_auth, NewState}, Out};
                {NewState, Out} ->
                    {{connected, set_last_time_active(true, NewState)}, Out}
            end;
        false ->
            lager:warning("invalid protocol version for ~p ~p",
                          [SubscriberId, ProtoVer]),
            connack_terminate(?UNSUPPORTED_PROTOCOL_VERSION, State)
    end.

data_in(Data, SessionState) when is_binary(Data) ->
    data_in(Data, SessionState, []).

data_in(Data, SessionState, OutAcc) ->
    case vmq_parser_mqtt5:parse(Data, max_incoming_msg_size()) of
        more ->
            {ok, SessionState, Data, lists:reverse(OutAcc)};
        {error, packet_exceeds_max_size} = E ->
            _ = vmq_metrics:incr(?MQTT5_INVALID_MSG_SIZE_ERROR),
            E;
        {error, Reason} ->
            {error, Reason, lists:reverse(OutAcc)};
        {Frame, Rest} ->
            case in(Frame, SessionState, true) of
                {stop, Reason, Out} ->
                    {stop, Reason, lists:reverse([Out|OutAcc])};
                {NewSessionState, {throttle, MS, Out}} ->
                    {throttle, MS, NewSessionState, Rest, lists:reverse([Out|OutAcc])};
                {NewSessionState, Out} when byte_size(Rest) == 0 ->
                    %% optimization
                    {ok, NewSessionState, Rest, lists:reverse([Out|OutAcc])};
                {NewSessionState, Out} ->
                    data_in(Rest, NewSessionState, [Out|OutAcc])
            end
    end.

msg_in(Msg, SessionState) ->
   case in(Msg, SessionState, false) of
       {stop, Reason, Out} ->
           {stop, Reason, lists:reverse(Out)};
       {NewSessionState, {throttle, _, Out}} ->
           %% we ignore throttling for the internal message flow
           {ok, NewSessionState, lists:reverse(Out)};
       {NewSessionState, Out} ->
           {ok, NewSessionState, lists:reverse(Out)}
   end.

%%% init --> pre_connect_auth | connected | --> terminate
in(Msg, {connected, State}, IsData) ->
    case connected(Msg, State) of
        {stop, _, _} = R -> R;
        {NewState, Out} ->
            {{connected, set_last_time_active(IsData, NewState)}, Out}
    end;
in(Msg, {pre_connect_auth, State}, IsData) ->
    case pre_connect_auth(Msg, State) of
        {stop, _, _} = R -> R;
        {pre_connect_auth, NewState, Out} ->
            {{pre_connect_auth, NewState}, Out};
        {NewState, Out} ->
            {{connected, set_last_time_active(IsData, NewState)}, Out}
    end.

serialise_frame(#mqtt5_publish{} = F) ->
    serialise_frame(F, #mqtt5_publish.properties, undefined);
serialise_frame(#mqtt5_puback{} = F) ->
    serialise_frame(F, #mqtt5_puback.properties, #mqtt5_puback.reason_code);
serialise_frame(#mqtt5_pubrec{} = F) ->
    serialise_frame(F, #mqtt5_pubrec.properties, #mqtt5_pubrec.reason_code);
serialise_frame(#mqtt5_pubrel{} = F) ->
    serialise_frame(F, #mqtt5_pubrel.properties, #mqtt5_pubrel.reason_code);
serialise_frame(#mqtt5_pubcomp{} = F) ->
    serialise_frame(F, #mqtt5_pubcomp.properties, #mqtt5_pubcomp.reason_code);
serialise_frame(#mqtt5_suback{} = F) ->
    serialise_frame(F, #mqtt5_suback.properties, #mqtt5_suback.reason_codes);
serialise_frame(#mqtt5_unsuback{} = F) ->
    serialise_frame(F, #mqtt5_unsuback.properties, #mqtt5_unsuback.reason_codes);
serialise_frame(#mqtt5_connack{} = F) ->
    serialise_frame(F, #mqtt5_connack.properties, #mqtt5_connack.reason_code);
serialise_frame(#mqtt5_disconnect{} = F) ->
    serialise_frame(F, #mqtt5_disconnect.properties, #mqtt5_disconnect.reason_code);
serialise_frame(#mqtt5_auth{} = F) ->
    serialise_frame(F, #mqtt5_auth.properties, #mqtt5_auth.reason_code);
serialise_frame(F) ->
    serialise_frame(F, undefined, undefined).

serialise_frame(F0, PropIndex, RCIndex) ->
    F1 = maybe_strip_problem_information(F0, PropIndex, RCIndex, request_problem_information()),
    maybe_reduce_packet_size(F1, PropIndex, max_outgoing_msg_size()).

maybe_strip_problem_information(F, _, _, true) -> F;
maybe_strip_problem_information(F, _, undefined, false) -> F;
maybe_strip_problem_information(F, PropIndex, RCIndex, false) ->
    case element(1, F) of
        mqtt5_publish -> F;
        mqtt5_connack -> F;
        mqtt5_disconnect -> F;
        _ ->
            case element(RCIndex, F) of
                ?M5_SUCCESS ->
                    F;
                _ ->
                    Properties0 = element(PropIndex, F),
                    Properties1 = maps:remove(p_reason_string, Properties0),
                    Properties2 = maps:remove(p_user_property, Properties1),
                    setelement(PropIndex, F, Properties2)
            end
    end.

maybe_reduce_packet_size(F0, _, undefined) -> vmq_parser_mqtt5:serialise(F0);
maybe_reduce_packet_size(F0, PropIndex, MaxPacketSize) ->
    B0 = vmq_parser_mqtt5:serialise(F0),
    case iolist_size(B0) > MaxPacketSize of
        true ->
            Properties0 = element(PropIndex, F0),
            Properties1 = maps:remove(p_reason_string, Properties0),
            Properties2 = maps:remove(p_user_property, Properties1),
            F1 = setelement(PropIndex, F0, Properties2),
            B1 = vmq_parser_mqtt5:serialise(F1),
            case iolist_size(B1) > MaxPacketSize of
                true ->
                    % still too big
                    [];
                false ->
                    B1
            end;
        false ->
            B0
    end.

maybe_initiate_trace(_Frame, undefined) ->
    ok;
maybe_initiate_trace(Frame, TraceFun) ->
    TraceFun(self(), Frame).

-spec pre_connect_auth(mqtt5_frame(), state()) ->
    {pre_connect_auth, state(), [binary()]} |
    {state(), [binary()]} |
    {stop, any(), [binary()]}.
pre_connect_auth(#mqtt5_auth{properties = #{p_authentication_method := AuthMethod,
                                            p_authentication_data := _} = Props,
                             reason_code = RC},
                 #state{enhanced_auth = #auth_data{method = AuthMethod,
                                                   data = ConnectFrame},
                        subscriber_id = SubscriberId,
                        username = UserName} = State) ->
    FilterProps =
        fun(M) -> maps:with([?P_AUTHENTICATION_METHOD,
                             ?P_AUTHENTICATION_DATA,
                             ?P_REASON_STRING], M)
        end,
    _ = vmq_metrics:incr({?MQTT5_AUTH_RECEIVED, rc2rcn(RC)}),
    case vmq_plugin:all_till_ok(on_auth_m5, [UserName, SubscriberId, Props]) of
        {ok, #{reason_code := ?SUCCESS,
               properties := #{?P_AUTHENTICATION_METHOD := AuthMethod} = Res}} ->
            %% we're done with enhanced auth on connect.
            NewAuthData = #auth_data{method = AuthMethod},
            check_connect(ConnectFrame,
                          FilterProps(Res),
                          State#state{enhanced_auth = NewAuthData});
        {ok, #{reason_code := ?CONTINUE_AUTHENTICATION,
               properties := #{?P_AUTHENTICATION_METHOD := AuthMethod} = Res}} ->
            _ = vmq_metrics:incr({?MQTT5_AUTH_SENT, ?CONTINUE_AUTHENTICATION}),
            Frame = #mqtt5_auth{reason_code = ?M5_CONTINUE_AUTHENTICATION,
                                properties = FilterProps(Res)},
            {pre_connect_auth, State, [serialise_frame(Frame)]};
        {error, #{reason_code := RCN} = Res} ->
            Props0 = maps:with([?P_REASON_STRING], maps:get(properties, Res, #{})),
            lager:warning(
              "can't continue enhanced auth with client ~p due to ~p",
              [State#state.subscriber_id, RCN]),
            terminate(RCN, Props0, State);
        {error, Reason} = E ->
            lager:warning(
              "can't continue enhanced auth with client ~p due to ~p",
              [State#state.subscriber_id, Reason]),
            terminate(E, State)
    end;
pre_connect_auth(#mqtt5_disconnect{properties=Properties,
                                   reason_code = RC}, State) ->
    _ = vmq_metrics:incr({?MQTT5_DISCONNECT_RECEIVED, disconnect_rc2rcn(RC)}),
    terminate_by_client(RC, Properties, State);
pre_connect_auth(_, State) ->
    terminate(?PROTOCOL_ERROR, State).


-spec connected(mqtt5_frame(), state()) ->
    {state(), [binary()]} |
    {state(), {throttle, duration_ms(), [binary()]}} |
    {stop, any(), [binary()]}.
connected(#mqtt5_publish{message_id=MessageId, topic=Topic,
                         qos=QoS, retain=IsRetain,
                         payload=Payload,
                         properties=Properties},
          #state{subscriber_id={MountPoint,_},
                 shared_subscription_policy=SGPolicy} = State) ->
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    _ = vmq_metrics:incr(?MQTT5_PUBLISH_RECEIVED),
    Ret =
    case Topic of
        [<<"$", _/binary>> |_] ->
            %% $SYS
            [];
        _ ->
            Msg = #vmq_msg{routing_key=Topic,
                           payload=Payload,
                           retain=unflag(IsRetain),
                           qos=QoS,
                           mountpoint=MountPoint,
                           msg_ref=msg_ref(),
                           sg_policy=SGPolicy,
                           properties=Properties,
                           expiry_ts=msg_expiration(Properties)},
            dispatch_publish(QoS, MessageId, Msg, State)
    end,
    case Ret of
        {error, recv_max_exceeded} ->
            terminate(?RECEIVE_MAX_EXCEEDED, State);
        Out when is_list(Out) ->
            case do_throttle(#{}, State) of
                false ->
                    {State, Out};
                ThrottleMs when is_integer(ThrottleMs) ->
                    {State, {throttle, ThrottleMs, Out}}
            end;
        {NewState, Out} when is_list(Out) ->
            case do_throttle(#{}, State) of
                false ->
                    {NewState, Out};
                ThrottleMs when is_integer(ThrottleMs) ->
                    {NewState, {throttle, ThrottleMs, Out}}
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
connected(#mqtt5_puback{message_id=MessageId, reason_code=RC}, #state{waiting_acks=WAcks} = State) ->
    %% qos1 flow
    _ = vmq_metrics:incr({?MQTT5_PUBACK_RECEIVED, rc2rcn(RC)}),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            Cnt = fc_decr_cnt(State#state.fc_send_cnt, puback),
            handle_waiting_msgs(State#state{fc_send_cnt=Cnt, waiting_acks=maps:remove(MessageId, WAcks)});
        not_found ->
            _ = vmq_metrics:incr(?MQTT5_PUBACK_INVALID_ERROR),
            {State, []}
    end;

connected(#mqtt5_pubrec{message_id=MessageId, reason_code=RC}, State) when RC < 16#80 ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    _ = vmq_metrics:incr({?MQTT5_PUBREC_RECEIVED,rc2rcn(RC)}),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            PubRelFrame = #mqtt5_pubrel{message_id=MessageId, reason_code=?M5_SUCCESS, properties=#{}},
            _ = vmq_metrics:incr({?MQTT5_PUBREL_SENT, ?SUCCESS}),
            {State#state{waiting_acks=maps:update(MessageId, PubRelFrame, WAcks)},
             [serialise_frame(PubRelFrame)]};
        #mqtt5_pubrel{message_id=MessageId} = PubRelFrame ->
            %% handle PUBREC retries from the client.
            _ = vmq_metrics:incr({?MQTT5_PUBREL_SENT, ?SUCCESS}),
            {State, [serialise_frame(PubRelFrame)]};
        not_found ->
            lager:debug("stopped connected session, due to unknown qos2 pubrec ~p", [MessageId]),
            _ = vmq_metrics:incr({?MQTT5_PUBREL_SENT, ?PACKET_ID_NOT_FOUND}),
            Frame = #mqtt5_pubrel{message_id=MessageId, reason_code=?M5_PACKET_ID_NOT_FOUND, properties=#{}},
            {State, [serialise_frame(Frame)]}
    end;
connected(#mqtt5_pubrec{message_id=MessageId, reason_code=ErrorRC}, State) ->
    %% qos2 flow with an error reason
    _ = vmq_metrics:incr({?MQTT5_PUBREC_RECEIVED, rc2rcn(ErrorRC)}),
    WAcks = maps:remove(MessageId, State#state.waiting_acks),
    Cnt = fc_decr_cnt(State#state.fc_send_cnt, pubrec),
    {State#state{waiting_acks=WAcks, fc_send_cnt=Cnt}, []};
connected(#mqtt5_pubrel{message_id=MessageId, reason_code=RC}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    _ = vmq_metrics:incr({?MQTT5_PUBREL_RECEIVED, rc2rcn(RC)}),
    case maps:get({qos2, MessageId} , WAcks, not_found) of
        {#mqtt5_pubrec{}, _Msg} ->
            Cnt = fc_decr_cnt(State#state.fc_receive_cnt, pubrel),
            {NewState, Msgs} =
            handle_waiting_msgs(
              State#state{
                fc_receive_cnt=Cnt,
                waiting_acks=maps:remove({qos2, MessageId}, WAcks)}),
            _ = vmq_metrics:incr({?MQTT5_PUBCOMP_SENT, ?SUCCESS}),
            {NewState, [serialise_frame(#mqtt5_pubcomp{message_id=MessageId,
                                                       reason_code=?M5_SUCCESS})|Msgs]};
        not_found ->
            _ = vmq_metrics:incr({?MQTT5_PUBCOMP_SENT, ?PACKET_ID_NOT_FOUND}),
            {State, [serialise_frame(#mqtt5_pubcomp{message_id=MessageId,
                                                    reason_code=?M5_PACKET_ID_NOT_FOUND})]}
    end;
connected(#mqtt5_pubcomp{message_id=MessageId, reason_code=RC}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    _ = vmq_metrics:incr({?MQTT5_PUBCOMP_RECEIVED, rc2rcn(RC)}),
    case maps:get(MessageId, WAcks, not_found) of
        #mqtt5_pubrel{} ->
            Cnt = fc_decr_cnt(State#state.fc_send_cnt, pubcomp),
            handle_waiting_msgs(State#state{
                                  fc_send_cnt=Cnt,
                                  waiting_acks=maps:remove(MessageId, WAcks)});
        not_found -> % error or wrong waiting_ack, definitely not well behaving client
            lager:debug("stopped connected session, due to qos2 pubrel missing ~p", [MessageId]),
            _ = vmq_metrics:incr(?MQTT5_PUBCOMP_INVALID_ERROR),
            %% TODOv5: we should probably not terminate normally here
            %% but use one of the new reason codes.
            terminate(?NORMAL_DISCONNECT, State)
    end;
connected(#mqtt5_subscribe{message_id=MessageId, topics=Topics, properties=Props0}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           cap_settings=CAPSettings} = State,
    _ = vmq_metrics:incr(?MQTT5_SUBSCRIBE_RECEIVED),
    SubTopics = vmq_mqtt_fsm_util:to_vmq_subtopics(Topics, get_sub_id(Props0)),
    OnAuthSuccess =
        fun(_User, _SubscriberId, MaybeChangedTopics, Props1) ->
                case vmq_reg:subscribe(CAPSettings#cap_settings.allow_subscribe, SubscriberId, MaybeChangedTopics) of
                    {ok, _QoSs} ->
                        vmq_plugin:all(on_subscribe_m5, [User, SubscriberId, MaybeChangedTopics, Props1]);
                    Res -> Res
                end
        end,
    case auth_on_subscribe(User, SubscriberId, SubTopics, Props0, OnAuthSuccess) of
        {ok, Modifiers} ->
            QoSs = topic_to_qos(maps:get(topics, Modifiers, [])),
            Props1 = maps:with(
                       [?P_REASON_STRING,
                        ?P_USER_PROPERTY], maps:get(properties, Modifiers, #{})),
            Frame = #mqtt5_suback{message_id=MessageId, reason_codes=QoSs, properties=Props1},
            _ = vmq_metrics:incr(?MQTT5_SUBACK_SENT),
            {State, [serialise_frame(Frame)]};
        {error, RCN} when RCN =:= ?NOT_AUTHORIZED;
                          RCN =:= ?CLIENT_IDENTIFIER_NOT_VALID;
                          RCN =:= ?UNSPECIFIED_ERROR;
                          RCN =:= ?QUOTA_EXCEEDED ->
            QoSs = [rcn2rc(RCN) || _ <- Topics],
            Frame = #mqtt5_suback{message_id=MessageId, reason_codes=QoSs, properties=#{}},
            _ = vmq_metrics:incr(?MQTT5_SUBSCRIBE_AUTH_ERROR),
            {State, [serialise_frame(Frame)]};
        {error, _Reason} ->
            %% cant subscribe due to overload or netsplit,
            %% Subscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr(?MQTT5_SUBSCRIBE_ERROR),
            {State, []}
    end;
connected(#mqtt5_unsubscribe{message_id=MessageId, topics=Topics, properties = Props0}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           cap_settings=CAPSettings} = State,
    _ = vmq_metrics:incr(?MQTT5_UNSUBSCRIBE_RECEIVED),
    OnSuccess =
        fun(_SubscriberId, MaybeChangedTopics) ->
                case vmq_reg:unsubscribe(CAPSettings#cap_settings.allow_unsubscribe, SubscriberId, MaybeChangedTopics) of
                    ok ->
                        vmq_plugin:all(on_topic_unsubscribed, [SubscriberId, MaybeChangedTopics]),
                        ok;
                    V -> V
                end
        end,
    case unsubscribe(User, SubscriberId, Topics, Props0, OnSuccess) of
        {ok, Props1} ->
            ReasonCodes = [?M5_SUCCESS || _ <- Topics],
            Frame = #mqtt5_unsuback{message_id=MessageId, reason_codes=ReasonCodes, properties=Props1},
            _ = vmq_metrics:incr(?MQTT5_UNSUBACK_SENT),
            {State, [serialise_frame(Frame)]};
        {error, _Reason} ->
            %% cant unsubscribe due to overload or netsplit,
            %% Unsubscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr(?MQTT5_UNSUBSCRIBE_ERROR),
            {State, []}
    end;
connected(#mqtt5_auth{properties=#{p_authentication_method := AuthMethod} = Props,
                      reason_code=RC},
          #state{enhanced_auth = #auth_data{method = AuthMethod},
                 subscriber_id = SubscriberId,
                 username = UserName} = State) ->
    FilterProps =
        fun(M) -> maps:with([?P_AUTHENTICATION_METHOD,
                             ?P_AUTHENTICATION_DATA,
                             ?P_REASON_STRING], M)
        end,
    _ = vmq_metrics:incr({?MQTT5_AUTH_RECEIVED, rc2rcn(RC)}),
    case vmq_plugin:all_till_ok(on_auth_m5, [UserName, SubscriberId, Props]) of
        {ok, #{reason_code := ?SUCCESS,
               properties :=
                   #{?P_AUTHENTICATION_METHOD := AuthMethod} = Res}} ->
            Frame = #mqtt5_auth{reason_code = ?M5_SUCCESS,
                                properties = FilterProps(Res)},
            {State, [serialise_frame(Frame)]};
        {ok, #{reason_code := ?CONTINUE_AUTHENTICATION,
               properties :=
                   #{?P_AUTHENTICATION_METHOD := AuthMethod} = Res}} ->
            _ = vmq_metrics:incr({?MQTT5_AUTH_SENT, ?CONTINUE_AUTHENTICATION}),
            Frame = #mqtt5_auth{reason_code = ?M5_CONTINUE_AUTHENTICATION,
                                properties = FilterProps(Res)},
            {State, [serialise_frame(Frame)]};
        {error, #{reason_code := RCN} = Res} ->
            Props0 = maps:with([?P_REASON_STRING], maps:get(properties, Res, #{})),
            lager:warning(
              "can't continue enhanced auth with client ~p due to ~p",
              [State#state.subscriber_id, RCN]),
            terminate(RCN, Props0, State);
        {error, Reason} = E ->
            lager:warning(
              "can't continue enhanced auth with client ~p due to ~p",
              [State#state.subscriber_id, Reason]),
            terminate(E, State)
    end;
connected(#mqtt5_auth{properties=#{p_authentication_method := GotAuthMethod},
                      reason_code=RC},
          #state{enhanced_auth = #auth_data{method = _WantAuthMethod}} = State) ->
    _ = vmq_metrics:incr({?MQTT5_AUTH_RECEIVED, rc2rcn(RC)}),
    terminate({error, {wrong_auth_method, GotAuthMethod}}, State);
connected(#mqtt5_pingreq{}, State) ->
    _ = vmq_metrics:incr(?MQTT5_PINGREQ_RECEIVED),
    Frame = #mqtt5_pingresp{},
    _ = vmq_metrics:incr(?MQTT5_PINGRESP_SENT),
    {State, [serialise_frame(Frame)]};
connected(#mqtt5_disconnect{properties=Properties, reason_code=RC}, State) ->
    _ = vmq_metrics:incr({?MQTT5_DISCONNECT_RECEIVED, disconnect_rc2rcn(RC)}),
    terminate_by_client(RC, Properties, State);
connected({disconnect, Reason}, State) ->
    lager:debug("stop due to disconnect", []),
    terminate(Reason, State);
connected(check_keepalive, #state{last_time_active=Last, keep_alive=KeepAlive,
                                  subscriber_id=SubscriberId, username=UserName} = State) ->
    Now = os:timestamp(),
    case timer:now_diff(Now, Last) > (1500000 * KeepAlive) of
        true ->
            lager:warning("client ~p with username ~p stopped due to keepalive expired", [SubscriberId, UserName]),
            _ = vmq_metrics:incr(?MQTT5_CLIENT_KEEPALIVE_EXPIRED),
            terminate(?KEEP_ALIVE_TIMEOUT, State);
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
    terminate({error, {unexpected_message, Unexpected}}, State).

-spec connack_terminate(reason_code_name(), state()) -> any().
connack_terminate(RCN, State) ->
    connack_terminate(RCN, #{}, State).

connack_terminate(RCN, Properties, _State) ->
    _ = vmq_metrics:incr({?MQTT5_CONNACK_SENT, RCN}),
    {stop, normal, [serialise_frame(#mqtt5_connack{session_present=false,
                                                   reason_code=rcn2rc(RCN),
                                                   properties=Properties})]}.

queue_down_terminate(shutdown, State) ->
    terminate(?NORMAL_DISCONNECT, State);
queue_down_terminate(Reason, #state{queue_pid=QPid} = State) ->
    terminate({error, {queue_down, QPid, Reason}}, State).

-spec terminate_by_client(reason_code(), properties(), state()) -> any().
terminate_by_client(RC, Props0, #state{queue_pid=QPid} = State) ->
    OldSInt = State#state.session_expiry_interval,
    NewSInt = maps:get(p_session_expiry_interval, Props0, 0),
    {Out, NewState} =
        case {OldSInt,NewSInt} of
            {0,NewSInt} when NewSInt > 0 ->
                {[gen_disconnect(?PROTOCOL_ERROR, #{})], State};
            {0,0} ->
                {[], State};
            _ ->
                %% the session expiry is legal, use the one we just
                %% received or fall back to the one from the connect
                %% packet.
                SInt = maps:get(p_session_expiry_interval, Props0, OldSInt),
                Props1 = maps:put(p_session_expiry_interval, SInt, Props0),
                QueueOpts = queue_opts_from_properties(Props1),
                vmq_queue:set_opts(QPid, QueueOpts),
                handle_waiting_acks_and_msgs(State),
                {[], State#state{session_expiry_interval = SInt}}
        end,
    case RC of
        ?M5_NORMAL_DISCONNECT ->
            do_nothing;
        ?M5_DISCONNECT_WITH_WILL_MSG ->
            schedule_last_will_msg(NewState);
        _ ->
            %% not really clear in the spec if we should send here,
            %% but we do for now.
            schedule_last_will_msg(NewState)
    end,
    {stop, normal, Out}.

-spec terminate(reason_code_name() | {error, any()}, state()) -> any().
terminate(Reason, State) ->
    terminate(Reason, #{}, State).

terminate(Reason, Props, #state{session_expiry_interval=SessionExpiryInterval,
                                subscriber_id=SubscriberId} = State) ->
    _ = case SessionExpiryInterval of
            0 -> ok;
            _ ->
                handle_waiting_acks_and_msgs(State)
        end,
    case suppress_lwt(Reason, State) of
        true ->
            lager:debug("last will and testament suppressed on session takeover for subscriber ~p",
                        [SubscriberId]);
        _ ->
            schedule_last_will_msg(State)
    end,
    Out =
        case Reason of
            {error, _} ->
                %% It is assumed that errors don't generally map to
                %% MQTT errors and as such we don't tell the client
                %% about them, we just drop the connection.
                [];
            _ ->
                [gen_disconnect(Reason, Props)]
        end,
    {stop, terminate_reason(Reason), Out}.

terminate_reason(Reason) ->
    vmq_mqtt_fsm_util:terminate_reason(Reason).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_enhanced_auth(#mqtt5_connect{properties=#{?P_AUTHENTICATION_METHOD := AuthMethod}=Props} = Frame0,
                    #state{subscriber_id = SubscriberId,
                           username=UserName} = State) ->
    FilterProps =
        fun(M) -> maps:with([?P_AUTHENTICATION_METHOD,
                             ?P_AUTHENTICATION_DATA,
                             ?P_REASON_STRING], M)
        end,
    case vmq_plugin:all_till_ok(on_auth_m5, [UserName, SubscriberId, Props]) of
        {ok, #{reason_code := ?M5_SUCCESS,
               properties :=
                   #{?P_AUTHENTICATION_METHOD := AuthMethod} = Res}} ->
            EnhancedAuth = #auth_data{method = AuthMethod},
            check_connect(Frame0, FilterProps(Res), State#state{enhanced_auth = EnhancedAuth});
        {ok, #{reason_code := ?CONTINUE_AUTHENTICATION,
               properties :=
                   #{?P_AUTHENTICATION_METHOD := AuthMethod} = Res}} ->
            EnhancedAuth = #auth_data{method = AuthMethod,
                                      data = Frame0},
            _ = vmq_metrics:incr({?MQTT5_AUTH_SENT, ?CONTINUE_AUTHENTICATION}),
            Frame1 = #mqtt5_auth{reason_code = ?M5_CONTINUE_AUTHENTICATION,
                                 properties = FilterProps(Res)},
            {pre_connect_auth, State#state{enhanced_auth = EnhancedAuth},
             [serialise_frame(Frame1)]};
        {error, #{reason_code := RCN} = Res} ->
            Props0 = maps:with([?P_REASON_STRING], maps:get(properties, Res, #{})),
            lager:warning(
              "can't continue enhanced auth with client ~p due to ~p",
              [State#state.subscriber_id, RCN]),
            connack_terminate(RCN, Props0, State);
        {error, Reason} ->
            lager:warning(
              "can't continue enhanced auth with client ~p due to ~p",
              [State#state.subscriber_id, Reason]),
            %% Without a specific reason code we don't send a connack
            %% message. This leaves this decision if a connack should
            %% be sent to the plugin developer
            {stop, normal, []}
    end;
check_enhanced_auth(F, State) ->
    %% No enhanced authentication
    check_connect(F, #{}, State).

check_connect(#mqtt5_connect{proto_ver=Ver, clean_start=CleanStart} = F, OutProps, State) ->
    CCleanStart = unflag(CleanStart),
    check_client_id(F, OutProps, State#state{clean_start=CCleanStart, proto_ver=Ver}).

check_client_id(#mqtt5_connect{} = Frame,
                OutProps,
                #state{username={preauth, UserNameFromCert}} = State) ->
    check_client_id(Frame#mqtt5_connect{username=UserNameFromCert},
                    OutProps,
                    State#state{username=UserNameFromCert});

check_client_id(#mqtt5_connect{client_id= <<>>, proto_ver=5} = F,
                OutProps,
                State) ->
    RandomClientId = random_client_id(),
    {MountPoint, _} = State#state.subscriber_id,
    SubscriberId = {MountPoint, RandomClientId},
    check_user(F#mqtt5_connect{client_id=RandomClientId},
               OutProps#{p_assigned_client_id => RandomClientId},
               State#state{subscriber_id=SubscriberId});
check_client_id(#mqtt5_connect{client_id=ClientId} = F,
                OutProps,
                #state{max_client_id_size=S} = State)
  when byte_size(ClientId) =< S ->
    {MountPoint, _} = State#state.subscriber_id,
    SubscriberId = {MountPoint, ClientId},
    check_user(F, OutProps, State#state{subscriber_id=SubscriberId});
check_client_id(#mqtt5_connect{client_id=Id}, _OutProps, State) ->
    lager:warning("invalid client id ~p", [Id]),
    connack_terminate(?CLIENT_IDENTIFIER_NOT_VALID, State).

check_user(#mqtt5_connect{username=User, password=Password, properties=Props} = F,
           OutProps,
           State) ->
    case State#state.allow_anonymous of
        false ->
            case auth_on_register(Password, Props, State#state{username=User}) of
                {ok, QueueOpts, OutProps0, NewState} ->
                    SessionExpiryInterval = maps:get(session_expiry_interval, QueueOpts, 0),
                    register_subscriber(F, maps:merge(OutProps, OutProps0), QueueOpts,
                                        NewState#state{session_expiry_interval=SessionExpiryInterval});
                {error, no_matching_hook_found} ->
                    lager:error("can't authenticate client ~p from ~s due to no_matching_hook_found",
                                [State#state.subscriber_id, peertoa(State#state.peer)]),
                    connack_terminate(?BAD_USERNAME_OR_PASSWORD, State);
                {error, Vals} when is_map(Vals) ->
                    RCN = maps:get(reason_code, Vals, ?BAD_USERNAME_OR_PASSWORD),
                    Props0 = maps:with([?P_REASON_STRING,
                                        ?P_SERVER_REF], maps:get(properties, Vals, #{})),
                    lager:warning(
                      "can't authenticate client ~p from ~s due to ~p",
                      [State#state.subscriber_id, peertoa(State#state.peer), RCN]),
                    connack_terminate(RCN, Props0, State);
                {error, Error} ->
                    %% can't authenticate due to other reason
                    lager:warning(
                      "can't authenticate client ~p from ~s due to ~p",
                      [State#state.subscriber_id, peertoa(State#state.peer), Error]),
                    connack_terminate(?BAD_USERNAME_OR_PASSWORD, State)
            end;
        true ->
            QueueOpts = queue_opts([], Props),
            SessionExpiryInterval = maps:get(session_expiry_interval, QueueOpts, 0),
            register_subscriber(F, OutProps, QueueOpts,
                                State#state{session_expiry_interval=SessionExpiryInterval})
    end.

register_subscriber(#mqtt5_connect{}=F, OutProps0,
                    QueueOpts, #state{peer=Peer, subscriber_id=SubscriberId, clean_start=CleanStart,
                                      cap_settings=CAPSettings, fc_receive_max_broker=ReceiveMax,
                                      username=User, def_opts=DOpts} = State) ->
    CoordinateRegs = maps:get(coordinate_registrations, DOpts, ?COORDINATE_REGISTRATIONS),
    case vmq_reg:register_subscriber(CAPSettings#cap_settings.allow_register, CoordinateRegs, SubscriberId, CleanStart, QueueOpts) of
        {ok, #{session_present := SessionPresent,
               initial_msg_id := MsgId,
               queue_pid := QPid}} ->
            monitor(process, QPid),
            _ = vmq_plugin:all(on_register_m5, [Peer, SubscriberId,
                                                User, OutProps0]),
            OutProps1 = maybe_set_receive_maximum(OutProps0, ReceiveMax),
            check_will(F, SessionPresent, OutProps1,
                       State#state{queue_pid=QPid,username=User, next_msg_id=MsgId});
        {error, Reason} ->
            lager:warning("can't register client ~p with username ~p due to ~p",
                          [SubscriberId, User, Reason]),
            connack_terminate(?SERVER_UNAVAILABLE, State)
    end.


check_will(#mqtt5_connect{lwt=undefined}, SessionPresent, OutProps, State) ->
    OutProps0 = maybe_add_topic_alias_max(OutProps, State),
    _ = vmq_metrics:incr({?MQTT5_CONNACK_SENT, ?SUCCESS}),
    {State, [serialise_frame(#mqtt5_connack{session_present=SessionPresent,
                                            reason_code=?M5_SUCCESS,
                                            properties=OutProps0})]};
check_will(#mqtt5_connect{
              lwt=#mqtt5_lwt{will_topic=Topic, will_msg=Payload, will_qos=Qos,
                             will_retain=IsRetain,will_properties=Properties}},
           SessionPresent,
           OutProps,
           State) ->
    #state{username=User, subscriber_id={MountPoint, _}=SubscriberId} = State,
    case maybe_apply_topic_alias_in(User, SubscriberId,
                                    #vmq_msg{routing_key=Topic,
                                             payload=Payload,
                                             msg_ref=msg_ref(),
                                             qos=Qos,
                                             retain=unflag(IsRetain),
                                             mountpoint=MountPoint,
                                             properties=Properties
                                            },
                                    fun(Msg, _, SessCtrl) -> {ok, Msg, SessCtrl} end,
                                    State) of
        {ok, Msg, _, NewState} ->
            OutProps0 = maybe_add_topic_alias_max(OutProps, State),
            _ = vmq_metrics:incr({?MQTT5_CONNACK_SENT, ?SUCCESS}),
            {NewState#state{will_msg=Msg},
             [serialise_frame(#mqtt5_connack{session_present=SessionPresent,
                                             reason_code=?M5_SUCCESS,
                                             properties=OutProps0})]};
        {error, Reason} ->
            lager:warning("can't authenticate last will
                          for client ~p due to ~p", [SubscriberId, Reason]),
            connack_terminate(?NOT_AUTHORIZED, State)
    end.

-spec maybe_apply_topic_alias_in(username(), subscriber_id(), msg(), aop_success_fun(), state()) ->
                                        {ok, msg(), session_ctrl(), state()} |
                                        {error, any()}.
maybe_apply_topic_alias_in(User, SubscriberId,
                        #vmq_msg{routing_key = [],
                                 properties = #{p_topic_alias := AliasId}} = Msg,
                        Fun, #state{topic_aliases_in = TA} = State) ->
    case maps:find(AliasId, TA) of
        {ok, AliasedTopic} ->
            case auth_on_publish(User, SubscriberId,
                                 remove_property(p_topic_alias, Msg#vmq_msg{routing_key = AliasedTopic}), Fun)
            of
                {ok, NewMsg, SessCtrl} ->
                    {ok, NewMsg, SessCtrl, State};
                {error, _} = E -> E
            end;
        error ->
            %% TODOv5 check the error code and add test case
            %% for it
            {error, ?M5_TOPIC_ALIAS_INVALID}
    end;
maybe_apply_topic_alias_in(User, SubscriberId,
                           #vmq_msg{properties = #{p_topic_alias := AliasId}} = Msg,
                           Fun, #state{topic_aliases_in = TA} = State) ->
    case auth_on_publish(User, SubscriberId, remove_property(p_topic_alias, Msg), Fun) of
        {ok, #vmq_msg{routing_key=MaybeChangedTopic}=NewMsg, SessCtrl} ->
            %% TODOv5: Should we check here that the topic isn't empty?
            {ok, NewMsg, SessCtrl, State#state{topic_aliases_in = TA#{AliasId => MaybeChangedTopic}}};
        {error, _E} = E -> E
    end;
maybe_apply_topic_alias_in(_User, _SubscriberId, #vmq_msg{routing_key = []},
                           _Fun, _State) ->
    %% Empty topic but no topic alias property (handled above).
    %% TODOv5 this is an error check it is handled correctly
    %% and add test-case
    {error, ?M5_TOPIC_FILTER_INVALID};
maybe_apply_topic_alias_in(User, SubscriberId, Msg,
                           Fun, State) ->
    %% normal publish
    case auth_on_publish(User, SubscriberId, remove_property(p_topic_alias, Msg), Fun) of
        {ok, NewMsg, SessCtrl} ->
            {ok, NewMsg, SessCtrl, State};
        {error, _} = E -> E
    end.

-spec maybe_apply_topic_alias_out(topic(), map(), state()) -> {topic(), map(), state()}.
maybe_apply_topic_alias_out(Topic, Properties, #state{topic_alias_max_out = 0} = State) ->
    {Topic, Properties, State};
maybe_apply_topic_alias_out(Topic, Properties, #state{topic_aliases_out = TA,
                                                       topic_alias_max_out = Max} = State) ->
    case maps:get(Topic, TA, not_found) of
        not_found ->
            case maps:size(TA) of
                S when S < Max ->
                    Alias = S + 1,
                    {Topic, Properties#{p_topic_alias => Alias},
                     State#state{topic_aliases_out = TA#{Topic => Alias}}};
                _ ->
                    %% Outgoing Alias Register is full
                    {Topic, Properties, State}
            end;
        Alias ->
            {<<>>, Properties#{p_topic_alias => Alias}, State}
    end.

remove_property(p_topic_alias, #vmq_msg{properties = Properties} = Msg) ->
    Msg#vmq_msg{properties = maps:remove(p_topic_alias, Properties)}.

auth_on_register(Password, Props, State) ->
    #state{clean_start=CleanStart, peer=Peer, cap_settings=CAPSettings,
           subscriber_id=SubscriberId, username=User} = State,
    HookArgs = [Peer, SubscriberId, User, Password, CleanStart, Props],
    case vmq_plugin:all_till_ok(auth_on_register_m5, HookArgs) of
        ok ->
            {ok, queue_opts([], Props), #{}, State};
        {ok, Args0} ->
            Args = maps:to_list(Args0),
            set_sock_opts(prop_val(tcp_opts, Args, [])),
            ChangedCAPSettings
            = CAPSettings#cap_settings{
                allow_register=?cap_val(allow_register, Args, CAPSettings),
                allow_publish=?cap_val(allow_publish, Args, CAPSettings),
                allow_subscribe=?cap_val(allow_subscribe, Args, CAPSettings),
                allow_unsubscribe=?cap_val(allow_unsubscribe, Args, CAPSettings)
               },

            ChangedProps = maps:with(
                             [?P_MAX_QOS,
                              ?P_RETAIN_AVAILABLE,
                              ?P_WILDCARD_SUBS_AVAILABLE,
                              ?P_SUB_IDS_AVAILABLE,
                              ?P_SHARED_SUBS_AVAILABLE], maps:get(properties, Args0,#{})),
            %% for efficiency reason the max_message_size isn't kept in the state
            set_max_incoming_msg_size(prop_val(max_message_size, Args, max_incoming_msg_size())),
            set_max_outgoing_msg_size(prop_val(max_packet_size, Args, max_outgoing_msg_size())),
            set_request_problem_information(prop_val(request_problem_info, Args, request_problem_information())),

            ChangedState = State#state{
                             subscriber_id=?state_val(subscriber_id, Args, State),
                             username=?state_val(username, Args, State),
                             clean_start=?state_val(clean_start, Args, State),
                             session_expiry_interval=?state_val(session_expiry_interval, Args, State),
                             reg_view=?state_val(reg_view, Args, State),
                             max_message_rate=?state_val(max_message_rate, Args, State),
                             fc_receive_max_broker=?state_val(fc_receive_max_broker, Args, State),
                             % we're not allowed to have a larger receive_max_client than the one
                             % provided by the client (which is already stored in fc_receive_max_client).
                             fc_receive_max_client=min(State#state.fc_receive_max_client, ?state_val(fc_receive_max_client, Args, State)),
                             shared_subscription_policy=?state_val(shared_subscription_policy, Args, State),
                             upgrade_qos=?state_val(upgrade_qos, Args, State),
                             topic_alias_max=?state_val(topic_alias_max, Args, State),
                             topic_aliases_in=?state_val(topic_aliases_in, Args, State),
                             cap_settings=ChangedCAPSettings
                            },
            {ok, queue_opts(Args, maps:merge(Props,ChangedProps)), ChangedProps, ChangedState};
        {error, Reason} ->
            {error, Reason}
    end.

set_sock_opts(Opts) ->
    self() ! {set_sock_opts, Opts}.

-spec auth_on_subscribe(username(), subscriber_id(),
                        [{topic(), qos()}], mqtt5_properties(),
                        fun((username(), subscriber_id(), [{topic(), subinfo()}], mqtt5_properties()) ->
                                   {ok, [qos() | not_allowed]} | {error, atom()})
                       ) -> {ok, auth_on_subscribe_m5_hook:sub_modifiers()} |
                            {error, atom()}.
auth_on_subscribe(User, SubscriberId, Topics, Props0, AuthSuccess) ->
    case vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                                [User, SubscriberId, Topics, Props0]) of
        ok ->
            AuthSuccess(User, SubscriberId, Topics, Props0),
            {ok, #{topics => Topics}};
        {ok, Modifiers} ->
            NewTopics = maps:get(topics, Modifiers, []),
            NewProps = maps:get(properties, Modifiers, #{}),
            AuthSuccess(User, SubscriberId, NewTopics, NewProps),
            {ok, Modifiers};
        {error, Error} ->
            {error, Error}
    end.

-type unsubsuccessfun() ::
        fun((subscriber_id(), [{topic(), qos()}]) ->
                   ok | {error, not_ready}).

-spec unsubscribe(username(), subscriber_id(), [topic()],
                  mqtt5_properties(), unsubsuccessfun())
                 -> {ok, mqtt5_properties()} | {error, not_ready}.
unsubscribe(User, SubscriberId, Topics0, Props0, UnsubFun) ->
    {Topics2, Props2} =
        case vmq_plugin:all_till_ok(on_unsubscribe_m5, [User, SubscriberId, Topics0, Props0]) of
            ok ->
                {Topics0, #{}};
            {ok, #{topics := [[W|_]|_] = Topics1} = Mods} when is_binary(W) ->
                Props1 =
                    maps:with([?P_REASON_STRING,
                               ?P_USER_PROPERTY], maps:get(properties, Mods, #{})),
                {Topics1, Props1};
            {error, _} ->
                {Topics0, #{}}
        end,
    case UnsubFun(SubscriberId, Topics2) of
        {error, _} = E ->
            E;
        ok ->
            {ok, Props2}
    end.

-spec auth_on_publish(username(), subscriber_id(), msg(), aop_success_fun()) ->
                             {ok, msg()} |
                             {ok, msg(), session_ctrl()} |
                             {error, atom() | {reason_code_name(),properties()}}.
auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=Topic,
                                             payload=Payload,
                                             qos=QoS,
                                             retain=IsRetain,
                                             properties=Properties} = Msg,
                AuthSuccess) ->

    HookArgs = [User, SubscriberId, QoS, Topic, Payload, unflag(IsRetain), Properties],
    case vmq_plugin:all_till_ok(auth_on_publish_m5, HookArgs) of
        ok ->
            AuthSuccess(Msg, HookArgs, #{});
        {ok, Args0} when is_map(Args0) ->
            #vmq_msg{mountpoint=MP} = Msg,
            ChangedTopic = maps:get(topic, Args0, Topic),
            ChangedPayload = maps:get(payload, Args0, Payload),
            ChangedQoS = maps:get(qos, Args0, QoS),
            ChangedIsRetain = maps:get(retain, Args0, IsRetain),
            ChangedMountpoint = maps:get(mountpoint, Args0, MP),
            ChangedProperties = maps:get(properties, Args0, Properties),
            HookArgs1 = [User, SubscriberId, ChangedQoS,
                         ChangedTopic, ChangedPayload,
                         ChangedIsRetain, ChangedProperties],
            SessCtrl = session_ctrl(Args0),
            AuthSuccess(Msg#vmq_msg{routing_key=ChangedTopic,
                                    payload=ChangedPayload,
                                    qos=ChangedQoS,
                                    retain=ChangedIsRetain,
                                    properties=ChangedProperties,
                                    mountpoint=ChangedMountpoint},
                        HookArgs1,
                        SessCtrl);
        {error, Vals} when is_map(Vals) ->
            RCN = maps:get(reason_code,Vals, ?NOT_AUTHORIZED),
            Props = maps:fold(
                      fun(reason_string,V,A) -> maps:put(?P_REASON_STRING,V,A);
                         (_,_,A) -> A
                      end,#{},Vals),
            {error, {RCN, Props}};
        {error, Re} ->
            lager:error("can't auth publish ~p due to ~p", [HookArgs, Re]),
            {error, not_allowed}
    end.

session_ctrl(Args) ->
    maps:with([throttle], Args).

-spec publish(cap_settings(), module(), username(), subscriber_id(), msg(), state()) ->
    {ok, msg(), session_ctrl(), state()} |
    {error, atom() |
    {reason_code_name(),properties()}}.
publish(CAPSettings, RegView, User, {_, ClientId} = SubscriberId, Msg, State) ->
    maybe_apply_topic_alias_in(User, SubscriberId, Msg,
                               fun(MaybeChangedMsg, HookArgs, SessCtrl) ->
                                       case on_publish_hook(vmq_reg:publish(CAPSettings#cap_settings.allow_publish, RegView, ClientId, MaybeChangedMsg),
                                                            HookArgs) of
                                           ok -> {ok, MaybeChangedMsg, SessCtrl};
                                           E -> E
                                       end
                               end,
                               State).

-spec on_publish_hook({ok, {integer(), integer()}} | {error, _}, list()) -> ok | {error, _}.
on_publish_hook({ok, _NumMatched}, HookParams) ->
    _ = vmq_plugin:all(on_publish_m5, HookParams),
    ok;
on_publish_hook(Other, _) -> Other.

-spec dispatch_publish(qos(), msg_id(), msg(), state()) ->
    list() |
    {state(), list()} |
    {state(), list(), session_ctrl()} |
    {error, recv_max_exceeded}.
dispatch_publish(Qos, MessageId, Msg, State) ->
    dispatch_publish_(Qos, MessageId, Msg, State).

dispatch_publish_(0, MessageId, Msg, State) ->
    dispatch_publish_qos0(MessageId, Msg, State);
dispatch_publish_(1, MessageId, Msg, State) ->
    CntOrErr = fc_incr_cnt(State#state.fc_receive_cnt, State#state.fc_receive_max_broker, dispatch_1),
    dispatch_publish_qos1(MessageId, Msg, CntOrErr, State);
dispatch_publish_(2, MessageId, Msg, State) ->
    CntOrErr = fc_incr_cnt(State#state.fc_receive_cnt, State#state.fc_receive_max_broker, dispatch_2),
    dispatch_publish_qos2(MessageId, Msg, CntOrErr, State).

-spec dispatch_publish_qos0(msg_id(), msg(), state()) ->
    list() |
    {state(), list(), session_ctrl()}.
dispatch_publish_qos0(_MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId,
           cap_settings=CAPSettings, reg_view=RegView} = State,
    case publish(CAPSettings, RegView, User, SubscriberId, Msg, State) of
        {ok, _, SessCtrl, NewState} ->
            {NewState, [], SessCtrl};
        {error, {_,_}} ->
            %% TODOv5: reflect reason code in metrics
            _ = vmq_metrics:incr(?MQTT5_PUBLISH_AUTH_ERROR),
            [];
        {error, _Reason} ->
            %% can't publish due to overload or netsplit
            _ = vmq_metrics:incr(?MQTT5_PUBLISH_ERROR),
            []
    end.

-spec dispatch_publish_qos1(msg_id(), msg(), error | receive_max(), state()) ->
    list() |
    {state(), list()} |
    {state(), list(), session_ctrl()} |
    {error, recv_max_exceeded}.
dispatch_publish_qos1(_, _, error, _) ->
    {error, recv_max_exceeded};
dispatch_publish_qos1(MessageId, Msg, _Cnt, State) ->
    % Ignore the Cnt as we send the Puback right away.
        #state{username=User, subscriber_id=SubscriberId,
               cap_settings=CAPSettings, reg_view=RegView} = State,
    case publish(CAPSettings, RegView, User, SubscriberId, Msg, State) of
        {ok, _, SessCtrl, NewState} ->
            _ = vmq_metrics:incr({?MQTT5_PUBACK_SENT, ?SUCCESS}),
            %% TODOv5: return properties in puback success
            {NewState,
             [serialise_frame(#mqtt5_puback{message_id=MessageId,
                                            reason_code=?M5_SUCCESS,
                                            properties=#{}})],
             SessCtrl};
        {error, {RCN, Props}} when is_map(Props) ->
            _ = vmq_metrics:incr({?MQTT5_PUBACK_SENT, RCN}),
            {State, [serialise_frame(#mqtt5_puback{message_id=MessageId,
                                                   reason_code=rcn2rc(RCN),
                                                   properties=Props})]};
        {error, not_allowed} ->
            _ = vmq_metrics:incr({?MQTT5_PUBACK_SENT, ?NOT_AUTHORIZED}),
            Frame = #mqtt5_puback{message_id=MessageId, reason_code=?M5_NOT_AUTHORIZED, properties=#{}},
            [serialise_frame(Frame)];
        {error, _Reason} ->
            %% can't publish due to overload or netsplit
            _ = vmq_metrics:incr({?MQTT5_PUBACK_SENT, ?IMPL_SPECIFIC_ERROR}),
            Frame = #mqtt5_puback{message_id=MessageId, reason_code=?M5_IMPL_SPECIFIC_ERROR, properties=#{}},
            [serialise_frame(Frame)]
    end.

-spec dispatch_publish_qos2(msg_id(), msg(), error | receive_max(), state()) ->
    list() |
    {state(), list()} |
    {state(), list(), session_ctrl()} |
    {error, recv_max_exceeded}.
dispatch_publish_qos2(_, _, error, _) ->
    {error, recv_max_exceeded};
dispatch_publish_qos2(MessageId, Msg, Cnt, State) ->
    #state{username=User, subscriber_id=SubscriberId,
           cap_settings=CAPSettings, reg_view=RegView, waiting_acks=WAcks} = State,
    case maps:get({qos2, MessageId}, WAcks, not_found) of
        not_found ->
            case publish(CAPSettings, RegView, User, SubscriberId, Msg, State) of
                {ok, NewMsg, SessCtrl, NewState} ->
                    Frame = #mqtt5_pubrec{message_id=MessageId, reason_code=?M5_SUCCESS, properties=#{}},
                    _ = vmq_metrics:incr({?MQTT5_PUBREC_SENT, ?SUCCESS}),
                    {NewState#state{
                       fc_receive_cnt=Cnt,
                       waiting_acks=maps:put({qos2, MessageId}, {Frame, NewMsg}, WAcks)},
                     [serialise_frame(Frame)],
                     SessCtrl};
                {error, {RCN, Props0}} when is_map(Props0) ->
                    _ = vmq_metrics:incr({?MQTT5_PUBREC_SENT, RCN}),
                    {State, [serialise_frame(#mqtt5_pubrec{message_id=MessageId,
                                                           reason_code=rcn2rc(RCN),
                                                           properties=Props0})]};
                {error, not_allowed} ->
                    _ = vmq_metrics:incr({?MQTT5_PUBREC_SENT, ?NOT_AUTHORIZED}),
                    Frame = #mqtt5_pubrec{message_id=MessageId, reason_code=?M5_NOT_AUTHORIZED, properties=#{}},
                    [serialise_frame(Frame)];
                {error, _Reason} ->
                    %% can't publish due to overload or netsplit
                    _ = vmq_metrics:incr({?MQTT5_PUBREC_SENT, ?UNSPECIFIED_ERROR}),
                    Frame = #mqtt5_pubrec{message_id=MessageId, reason_code=?M5_UNSPECIFIED_ERROR, properties=#{}},
                    [serialise_frame(Frame)]
            end;
        _Frame ->
            Frame = #mqtt5_pubrec{message_id=MessageId, reason_code=?M5_SUCCESS, properties=#{}},
            [serialise_frame(Frame)]
    end.


-spec handle_waiting_acks_and_msgs(state()) -> ok.
handle_waiting_acks_and_msgs(State) ->
    #state{waiting_acks=WAcks, waiting_msgs=WMsgs, queue_pid=QPid, next_msg_id=NextMsgId} = State,
    MsgsToBeDeliveredNextTime =
    lists:foldl(fun ({{qos2, _}, _}, Acc) ->
                      Acc;
                  ({MsgId, #mqtt5_pubrel{} = Frame}, Acc) ->
                      %% unacked PUBREL Frame
                      [{deliver_pubrel, {MsgId, Frame}} | Acc];
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
                                %% item at the to)
                                maps:to_list(WAcks)
                               )
                 )),
    catch vmq_queue:set_last_waiting_acks(QPid, MsgsToBeDeliveredNextTime, NextMsgId).

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
    handle_messages(Rest, [Frame|Frames], PubCnt, NewState, Waiting);
handle_messages([#deliver{} = Obj|Rest], Frames, PubCnt, State, Waiting) ->
    case fc_incr_cnt(State#state.fc_send_cnt, State#state.fc_receive_max_client, handle_messages) of
        error ->
            % reached outgoing flow control max, queue up rest of messages
            handle_messages(Rest, Frames, PubCnt, State, [Obj|Waiting]);
        Cnt ->
            {Frame, NewState} = prepare_frame(Obj, State#state{fc_send_cnt=Cnt}),
            handle_messages(Rest, [Frame|Frames], PubCnt + 1, NewState, Waiting)
    end;
handle_messages([{deliver_pubrel, {MsgId, #mqtt5_pubrel{} = Frame}}|Rest], Frames, PubCnt, State0, Waiting) ->
    %% this is called when a pubrel is retried after a client reconnects
    #state{waiting_acks=WAcks} = State0,
    _ = vmq_metrics:incr({?MQTT5_PUBREL_SENT, ?SUCCESS}),
    State1 = State0#state{waiting_acks=maps:put(MsgId, Frame, WAcks)},
    handle_messages(Rest, [serialise_frame(Frame)|Frames],
                    PubCnt, State1, Waiting);
handle_messages([], [], _, State, Waiting) ->
    {State, [], Waiting};
handle_messages([], Frames, PubCnt, State, Waiting) ->
    _ = vmq_metrics:incr(?MQTT5_PUBLISH_SENT, PubCnt),
    {State, Frames, Waiting}.

prepare_frame(#deliver{qos=QoS, msg_id=MsgId, msg=Msg}, State0) ->
    #state{username=User, subscriber_id=SubscriberId, waiting_acks=WAcks} = State0,
    #vmq_msg{routing_key=Topic0,
             payload=Payload0,
             retain=IsRetained,
             dup=IsDup,
             qos=MsgQoS,
             properties=Props0,
             expiry_ts=ExpiryTS} = Msg,
    NewQoS = maybe_upgrade_qos(QoS, MsgQoS, State0),
    {Topic1, Payload1, Props2} =
    case on_deliver_hook(User, SubscriberId, QoS, Topic0, Payload0, IsRetained, Props0) of
        {error, _} ->
            %% no on_deliver hook specified... that's ok
            {Topic0, Payload0, Props0};
        ok ->
            {Topic0, Payload0, Props0};
        {ok, Args} when is_map(Args) ->
            ChangedTopic = maps:get(topic, Args, Topic0),
            ChangedPayload = maps:get(payload, Args, Payload0),
            Props1 = maps:get(properties, Args, Props0),
            {ChangedTopic, ChangedPayload, Props1}
    end,
    {Topic2, Props3, State1} = maybe_apply_topic_alias_out(Topic1, Props2, State0),
    {OutgoingMsgId, State2} = get_msg_id(NewQoS, MsgId, State1),
    Frame = serialise_frame(#mqtt5_publish{message_id=OutgoingMsgId,
                                           topic=Topic2,
                                           qos=NewQoS,
                                           retain=IsRetained,
                                           dup=IsDup,
                                           payload=Payload1,
                                           properties=update_expiry_interval(Props3, ExpiryTS)}),
    case Frame of
        [] ->
            % frame discarded due to max packet size limit
            Reason = max_packet_size_exceeded,
            vmq_plugin:all(on_message_drop, [SubscriberId,
                                             fun() -> {Topic1, NewQoS, Payload1, Props0} end,
                                             Reason]),
            {[], State2};
        _ when NewQoS == 0 ->
            {Frame, State2};
        _ ->
            {Frame,
             State2#state{
               waiting_acks=maps:put(OutgoingMsgId,
                                     Msg#vmq_msg{qos=NewQoS}, WAcks)}}
    end.

on_deliver_hook(User, SubscriberId, QoS, Topic, Payload, IsRetain, Props) ->
    HookArgs0 = [User, SubscriberId, Topic, Payload, Props],
    case vmq_plugin:all_till_ok(on_deliver_m5, HookArgs0) of
        {error, _} ->
            HookArgs1 = [User, SubscriberId, QoS, Topic, Payload, IsRetain, Props],
            vmq_plugin:all_till_ok(on_deliver_m5, HookArgs1);
        Other -> Other
    end.

suppress_lwt(?SESSION_TAKEN_OVER,
             #state{will_msg=WillMsg,
                    def_opts=#{suppress_lwt_on_session_takeover := true}})
  when WillMsg =/= undefined->
    true;
suppress_lwt(_,_) ->
    false.

-spec schedule_last_will_msg(state()) -> ok.
schedule_last_will_msg(#state{will_msg=undefined}) -> ok;
schedule_last_will_msg(#state{subscriber_id={_, ClientId} = SubscriberId, username=User,
                               will_msg=Msg, reg_view=RegView, cap_settings=CAPSettings,
                               queue_pid=QueuePid,
                               session_expiry_interval=SessionExpiryInterval}) ->
    LastWillFun =
        fun() ->
                #vmq_msg{qos=QoS, routing_key=Topic, payload=Payload, retain=IsRetain} = Msg,
                HookArgs = [User, SubscriberId, QoS, Topic, Payload, IsRetain],
                _ = on_publish_hook(vmq_reg:publish(CAPSettings#cap_settings.allow_publish,
                                                    RegView, ClientId, filter_outgoing_pub_props(Msg)), HookArgs)
        end,
    case get_last_will_delay(Msg) of
        Delay when (Delay > 0) and (SessionExpiryInterval > 0) ->
            vmq_queue:set_delayed_will(QueuePid, LastWillFun, Delay);
        _ ->
            LastWillFun()
    end.

get_last_will_delay(#vmq_msg{properties = #{p_will_delay_interval := Delay}}) ->
    MaxDuration = vmq_config:get_env(max_last_will_delay, 0),
    min(MaxDuration, Delay);
get_last_will_delay(_) -> 0.

%% The MQTT specification requires that the QoS of a message delivered to a
%% subscriber is never upgraded to match the QoS of the subscription. If
%% upgrade_outgoing_qos is set true, messages sent to a subscriber will always
%% match the QoS of its subscription. This is a non-standard option not provided
%% for by the spec.
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


set_keepalive_check_timer(0) -> ok;
set_keepalive_check_timer(KeepAlive) ->
    %% This allows us to heavily reduce start and cancel timers,
    %% however we're losing precision. But that's ok for the keepalive timer.
    _ = send_after(KeepAlive * 750, check_keepalive),
    ok.

-spec send_after(non_neg_integer(), any()) -> reference().
send_after(Time, Msg) ->
    vmq_mqtt_fsm_util:send_after(Time, Msg).

-spec do_throttle(session_ctrl(), state()) -> false | duration_ms().
do_throttle(#{throttle := ThrottleMs}, _) -> ThrottleMs;
do_throttle(_, #state{max_message_rate=0}) -> false;
do_throttle(_, #state{max_message_rate=Rate}) ->
    case vmq_metrics:check_rate(msg_in_rate, Rate) of
        true -> false;
        _ -> 1000
    end.

set_last_time_active(true, State) ->
    Now = os:timestamp(),
    State#state{last_time_active=Now};
set_last_time_active(false, State) ->
    State.

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
prop_val(Key, Args, Default) when is_map(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_map/1);
prop_val(Key, Args, Default) when is_binary(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_binary/1).

prop_val(Key, Args, Default, Validator) ->
    case proplists:get_value(Key, Args) of
        undefined -> Default;
        Val -> case Validator(Val) of
                   true -> Val;
                   false -> Default
               end
    end.

queue_opts_from_properties(Properties) ->
    maps:fold(
      fun(p_session_expiry_interval, Val, Acc) ->
              Acc#{session_expiry_interval => Val,
                   cleanup_on_disconnect => (Val == 0)};
         (_,_,Acc) -> Acc
      end, #{cleanup_on_disconnect => true}, Properties).

queue_opts(Args, Properties) ->
    PropertiesOpts = queue_opts_from_properties(Properties),
    Opts = maps:from_list(Args),
    Opts1 = maps:merge(PropertiesOpts, Opts),
    maps:merge(vmq_queue:default_opts(), Opts1).

unflag(true) -> true;
unflag(false) -> false;
unflag(?true) -> true;
unflag(?false) -> false.

msg_ref() ->
    GUID =
    case get(guid) of
        undefined ->
            {{node(), self(), erlang:timestamp()}, 0};
        {S, I} ->
            {S, I + 1}
    end,
    put(guid, GUID),
    erlang:md5(term_to_binary(GUID)).

max_incoming_msg_size() ->
    case get(max_incoming_msg_size) of
        undefined ->
            %% no limit
            0;
        V -> V
    end.

max_outgoing_msg_size() ->
    get(max_outgoing_msg_size).

request_problem_information() ->
    get(request_problem_information).

set_max_incoming_msg_size(MaxMsgSize) when MaxMsgSize >= 0 ->
    put(max_incoming_msg_size, MaxMsgSize),
    MaxMsgSize.

set_max_outgoing_msg_size(Max)
  when (Max == undefined) or ((Max > 0) and (Max =< ?MAX_PACKET_SIZE)) ->
    put(max_outgoing_msg_size, Max),
    Max.

set_request_problem_information(RequestProblemInfo) when is_boolean(RequestProblemInfo) ->
    put(request_problem_information, RequestProblemInfo).

info(Pid, Items) ->
    Ref = make_ref(),
    CallerRef = {Ref, self()},
    MRef = monitor(process, Pid),
    Pid ! {info_req, CallerRef, Items},
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
get_info_items([waiting_acks|Rest], State, Acc) ->
    Size = maps:size(State#state.waiting_acks),
    get_info_items(Rest, State,
                   [{waiting_acks, Size}|Acc]);
get_info_items([_|Rest], State, Acc) ->
    get_info_items(Rest, State, Acc);
get_info_items([], _, Acc) -> Acc.

-spec gen_disconnect(reason_code_name(), mqtt5_properties()) -> vmq_parser_mqtt5:serialized().
gen_disconnect(RCN, Props) ->
    gen_disconnect_(RCN, Props).

gen_disconnect_(RCN, Props) ->
    _ = vmq_metrics:incr({?MQTT5_DISCONNECT_SENT, RCN}),
    serialise_frame(#mqtt5_disconnect{reason_code = rcn2rc(RCN), properties = Props}).

msg_expiration(#{p_message_expiry_interval := ExpireAfter}) ->
    {expire_after, ExpireAfter};
msg_expiration(_) ->
    undefined.

update_expiry_interval(Properties, undefined) -> Properties;
update_expiry_interval(Properties, {_, Remaining}) ->
    Properties#{p_message_expiry_interval => Remaining}.

maybe_add_topic_alias_max(Props, #state{topic_alias_max=0}) ->
    Props;
maybe_add_topic_alias_max(Props, #state{topic_alias_max=Max}) ->
    Props#{p_topic_alias_max => Max}.

maybe_get_topic_alias_max(#{p_topic_alias_max := Max}, ConfigMax) when Max > 0 -> min(Max, ConfigMax);
maybe_get_topic_alias_max(_, ConfigMax) -> ConfigMax.

maybe_set_receive_maximum(Props, ?FC_RECEIVE_MAX) -> Props;
maybe_set_receive_maximum(Props, ConfigMax)
  when (ConfigMax > 0) and (ConfigMax < ?FC_RECEIVE_MAX) ->
    Props#{p_receive_max => ConfigMax}.

maybe_get_receive_maximum(#{p_receive_max := Max}, ConfigMax)
  when (ConfigMax > 0) and (ConfigMax =< ?FC_RECEIVE_MAX) -> min(Max, ConfigMax);
maybe_get_receive_maximum(_, ConfigMax)
  when (ConfigMax > 0) and (ConfigMax =< ?FC_RECEIVE_MAX) -> ConfigMax.

maybe_get_maximum_packet_size(#{p_max_packet_size := Max}, undefined)
  when (Max > 0) -> Max;
maybe_get_maximum_packet_size(#{p_max_packet_size := Max}, ConfigMax)
  when (Max > 0) and (ConfigMax > 0) and (ConfigMax =< ?MAX_PACKET_SIZE) -> min(Max, ConfigMax);
maybe_get_maximum_packet_size(_, undefined) -> undefined;
maybe_get_maximum_packet_size(_, ConfigMax)
  when (ConfigMax > 0) and (ConfigMax =< ?MAX_PACKET_SIZE) -> ConfigMax.

maybe_get_request_problem_information(#{p_request_problem_info := RequestProblemInfo}, _) -> unflag(RequestProblemInfo);
maybe_get_request_problem_information(_, RequestProblemInfo) -> RequestProblemInfo.

fc_incr_cnt(ConfigMax, ConfigMax, What) -> fc_log(incr, What, {ConfigMax, ConfigMax}), error;
fc_incr_cnt(Cnt, ConfigMax, What) when Cnt < ConfigMax -> fc_log(incr, What, {Cnt, ConfigMax}), Cnt + 1.

fc_decr_cnt(0, What) -> fc_log(decr, What, 0), 0;
fc_decr_cnt(Cnt, What) when Cnt > 0 -> fc_log(decr, What, Cnt - 1), Cnt - 1.

fc_log(_Action, _Location, _Cnt) ->
    noop.
    %io:format(user, "fc[~p]: ~p ~p~n", [Location, Action, Cnt]).

filter_outgoing_pub_props(#vmq_msg{properties=Props} = Msg) when map_size(Props) =:= 0 ->
    Msg;
filter_outgoing_pub_props(#vmq_msg{properties=Props} = Msg) ->
    %% make sure we don't forward any properties which we're not
    %% allowed to.
    Msg#vmq_msg{properties=
                    maps:with([p_payload_format_indicator,
                               p_response_topic,
                               p_correlation_data,
                               p_user_property,
                               p_content_type,
                               p_message_expiry_interval
                              ], Props)}.

get_sub_id(#{p_subscription_id := [SubId]}) ->
    SubId;
get_sub_id(_) -> undefined.

-spec topic_to_qos([subscription()]) -> [qos()].
topic_to_qos(Topics) ->
    lists:map(
      fun({_T, QoS}) when is_integer(QoS) ->
              QoS;
         ({_T, {QoS, _}}) ->
              QoS
      end, Topics).

disconnect_rc2rcn(0) ->
    ?NORMAL_DISCONNECT;
disconnect_rc2rcn(RC) ->
    rc2rcn(RC).

-spec rcn2rc(reason_code_name()) -> reason_code().
rcn2rc(RCN) ->
    vmq_parser_mqtt5:rcn2rc(RCN).

-spec rc2rcn(reason_code()) -> reason_code_name().
rc2rcn(RC) ->
    vmq_parser_mqtt5:rc2rcn(RC).

set_defopt(Key, Default, Map) ->
    case vmq_config:get_env(Key, Default) of
        Default ->
            Map;
        NonDefault ->
            maps:put(Key, NonDefault, Map)
    end.

peertoa({_IP, _Port} = Peer) ->
    vmq_mqtt_fsm_util:peertoa(Peer).
