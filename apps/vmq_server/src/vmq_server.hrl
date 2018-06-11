-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-type routing_key()         :: [binary()].
-type msg_ref()             :: binary().

-type plugin_id()       :: {plugin, atom(), pid()}.

-type msg_expiry_ts() :: {expire_after, non_neg_integer()}
                       | {non_neg_integer(), non_neg_integer()}.

-type sg_policy() :: prefer_local | local_only | random.
-record(vmq_msg, {
          msg_ref               :: msg_ref() | 'undefined', % OTP-12719
          routing_key           :: routing_key() | 'undefined',
          payload               :: payload() | 'undefined',
          retain=false          :: flag(),
          dup=false             :: flag(),
          qos                   :: qos(),
          mountpoint            :: mountpoint(),
          persisted=false       :: flag(),
          sg_policy=prefer_local:: sg_policy(),
          %% TODOv5: need to import the mqtt5 property typespec?
          properties=#{}        :: map(),
          expiry_ts             :: undefined
                                 | msg_expiry_ts()
         }).
-type msg()             :: #vmq_msg{}.

-type subscription() :: {topic(), subinfo()}.
-define(INTERNAL_CLIENT_ID, '$vmq_internal_client_id').

%% These reason codes are used internally within vernemq and are not
%% *real* MQTT reason codes.
-define(DISCONNECT_KEEP_ALIVE,    disconnect_keep_alive).
-define(DISCONNECT_MIGRATION,     disconnect_migration).

-type disconnect_reasons() ::
        ?NOT_AUTHORIZED |
        ?NORMAL_DISCONNECT |
        ?SESSION_TAKEN_OVER |
        ?ADMINISTRATIVE_ACTION |
        ?DISCONNECT_KEEP_ALIVE |
        ?DISCONNECT_MIGRATION |
        ?BAD_AUTHENTICATION_METHOD |
        ?PROTOCOL_ERROR |
        ?RECEIVE_MAX_EXCEEDED.
