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

-module(vmq_listener_cli).
-export([register_server_cli/0]).

register_server_cli() ->
    clique:register_usage(["vmq-admin", "listener"], vmq_listener_usage()),
    clique:register_usage(["vmq-admin", "listener", "start"],
                          vmq_listener_start_usage()),
    clique:register_usage(["vmq-admin", "listener", "stop"],
                          vmq_listener_stop_usage()),
    clique:register_usage(["vmq-admin", "listener", "delete"],
                          vmq_listener_delete_usage()),
    clique:register_usage(["vmq-admin", "listener", "restart"],
                          vmq_listener_restart_usage()),
    vmq_listener_start_cmd(),
    vmq_listener_stop_cmd(),
    vmq_listener_delete_cmd(),
    vmq_listener_restart_cmd(),
    vmq_listener_show_cmd().

vmq_listener_start_cmd() ->
    Cmd = ["vmq-admin", "listener", "start"],
    KeySpecs = [{port, [{typecast, fun parse_port/1}]},
                {address, [{typecast, fun parse_addr/1}]}],
    FlagSpecs = [{mountpoint, [{shortname, "m"},
                               {longname, "mountpoint"},
                               {typecast, fun(MP) -> MP end}]},
                 {max_connections, [{longname, "max_connections"},
                               {typecast, fun
                                              ("infinity") -> infinity;
                                              (N) ->
                                                    list_to_integer(N)
                                          end}]},
                 {nr_of_acceptors, [{longname, "nr_of_acceptors"},
                                    {typecast, fun(N) -> list_to_integer(N) end}]},
                 {websocket, [{shortname, "ws"},
                              {longname, "websocket"}]},
                 {proxy_protocol, [{longname, "proxy_protocol"}]},
                 {proxy_protocol_use_cn_as_username, [{longname, "proxy_protocol_use_cn_as_username"},
                                                      {typecast, fun("true") -> true;
                                                                    (_) -> false
                                                                 end}]},
                 {allowed_protocol_versions, [{longname, "allowed_protocol_versions"},
                                              {typecast, fun(ProtoVers) ->
                                                                 {ok, T, _} = erl_scan:string("[" ++ ProtoVers ++ "]."),
                                                                 {ok, Term} = erl_parse:parse_term(T),
                                                                 Term
                                                         end}]},
                 {http, [{shortname, "http"},
                         {longname, "http"}]},
                 {ssl, [{longname, "ssl"}]},
                 {cafile, [{longname, "cafile"},
                           {typecast, fun(FileName) ->
                                              case filelib:is_file(FileName) of
                                                  true -> FileName;
                                                  false ->
                                                      {error, {invalid_flag_value,
                                                               {cafile, FileName}}}
                                              end
                                      end}]},
                 {certfile, [{longname, "certfile"},
                             {typecast, fun(FileName) ->
                                                case filelib:is_file(FileName) of
                                                    true -> FileName;
                                                    false ->
                                                        {error, {invalid_flag_value,
                                                                 {certfile, FileName}}}
                                                end
                                        end}]},
                 {keyfile, [{longname, "keyfile"},
                            {typecast, fun(FileName) ->
                                               case filelib:is_file(FileName) of
                                                   true -> FileName;
                                                   false ->
                                                       {error, {invalid_flag_value,
                                                                {keyfile, FileName}}}
                                               end
                                       end}]},
                 {ciphers, [{longname, "ciphers"},
                            {typecast, fun(C) -> C end}]},
                 {eccs, [{longname, "eccs"},
                            {typecast, fun(C) -> C end}]},
                 {crlfile, [{longname, "crlfile"},
                            {typecast, fun(FileName) ->
                                               case filelib:is_file(FileName) of
                                                   true -> FileName;
                                                   false ->
                                                       {error, {invalid_flag_value,
                                                                {crlfile, FileName}}}
                                               end
                                       end}]},
                 {require_certificate, [{longname, "require_certificate"}]},
                 {tls_version, [{longname, "tls_version"},
                                {typecast, fun("tlsv1") -> tlsv1;
                                              ("tlsv1.1") -> 'tlsv1.1';
                                              ("tlsv1.2") -> 'tlsv1.2';
                                              (V) ->
                                                   {error, {invalid_flag_value,
                                                            {'tls-version', V}}}
                                           end}]},
                 {use_identity_as_username, [{longname, "use_identity_as_username"}]},
                 {config_mod, [{longname, "config_mod"},
                               {typecast, fun(M) -> list_to_existing_atom(M) end}]},
                 {config_fun, [{longname, "config_fun"},
                               {typecast, fun(F) -> list_to_existing_atom(F) end}]}
                ],
    Callback =
    fun(_, [{address, Addr},{port, Port}], Flags) ->
            maybe_start_listener(Addr, Port, Flags);
       (_, [{port, Port}, {address, Addr}], Flags) ->
            maybe_start_listener(Addr, Port, Flags);
       (_, _, _) ->
            Text = clique_status:text(vmq_listener_start_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

maybe_start_listener(Addr, Port, Flags) ->
    IsWebSocket = lists:keymember(websocket, 1, Flags),
    IsPlainHTTP = lists:keymember(http, 1, Flags),
    IsSSL = lists:keymember(ssl, 1, Flags),
    NewOpts1 = cleanup_flags(Flags, []),
    case IsSSL of
        true when IsWebSocket ->
            start_listener(mqttwss, Addr, Port, NewOpts1);
        true when IsPlainHTTP ->
            start_listener(https, Addr, Port, NewOpts1);
        true ->
            start_listener(mqtts, Addr, Port, NewOpts1);
        false when IsWebSocket ->
            start_listener(mqttws, Addr, Port, NewOpts1);
        false when IsPlainHTTP ->
            start_listener(http, Addr, Port, NewOpts1);
        false ->
            start_listener(mqtt, Addr, Port, NewOpts1)
    end.

start_listener(Type, Addr, Port, Opts) ->
    case vmq_ranch_config:start_listener(Type, Addr, Port, Opts) of
        ok ->
            ListenerKey = {Addr, Port},
            ListenerConfig = vmq_config:get_env(listeners),
            ConfigForType = proplists:get_value(Type, ListenerConfig, []),
            NewConfigForType = [{ListenerKey, Opts}
                                |lists:keydelete(ListenerKey, 1, ConfigForType)],
            NewListenerConfig = [{Type, NewConfigForType}
                                 |lists:keydelete(Type, 1, ListenerConfig)],
            vmq_config:set_env(listeners, NewListenerConfig, false),
            [clique_status:text("Done")];
        {error, Reason} ->
            lager:warning("can't start listener ~p ~p ~p ~p due to ~p",
                          [Type, Addr, Port, Opts, Reason]),
            Text = io_lib:format("can't start listener due to '~p'", [Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end.

vmq_listener_stop_cmd() ->
    Cmd = ["vmq-admin", "listener", "stop"],
    KeySpecs = [{port, [{typecast, fun parse_port/1}]},
                {address, [{typecast, fun parse_addr/1}]}],
    FlagSpecs = [{kill, [{shortname, "k"},
                         {longname, "kill_sessions"}]}],
    Callback =
    fun(_, [{address, Addr},{port, Port}], Flags) ->
            stop_listener(Addr, Port, Flags);
       (_, [{port, Port}, {address, Addr}], Flags) ->
            stop_listener(Addr, Port, Flags);
       (_, _, _) ->
            Text = clique_status:text(vmq_listener_stop_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

stop_listener(Addr, Port, Flags) ->
    IsKill = lists:keymember(kill, 1, Flags),
    case vmq_ranch_config:stop_listener(Addr, Port, IsKill) of
        ok ->
            [clique_status:text("Done")];
        {error, Reason} ->
            Text = io_lib:format("can't stop listener due to '~p'", [Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end.

vmq_listener_delete_cmd() ->
    Cmd = ["vmq-admin", "listener", "delete"],
    KeySpecs = [{port, [{typecast, fun parse_port/1}]},
                {address, [{typecast, fun parse_addr/1}]}],
    FlagSpecs = [],
    Callback =
    fun(_, [{address, Addr},{port, Port}], []) ->
            delete_listener(Addr, Port);
       (_, [{port, Port}, {address, Addr}], []) ->
            delete_listener(Addr, Port);
       (_, _, _) ->
            Text = clique_status:text(vmq_listener_delete_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

delete_listener(Addr, Port) ->
    vmq_ranch_config:delete_listener(Addr, Port),
    ListenerKey = {Addr, Port},
    ListenerConfig = vmq_config:get_env(listeners),
    NewListenerConfig =
        lists:foldl(
          fun({Type, ConfigForType}, Acc) ->
                  NewConfigForType = lists:keydelete(ListenerKey, 1, ConfigForType),
                  [{Type, NewConfigForType}|Acc]
          end, [], ListenerConfig),
    vmq_config:set_env(listeners, NewListenerConfig, false),
    [clique_status:text("Done")].

vmq_listener_restart_cmd() ->
    Cmd = ["vmq-admin", "listener", "restart"],
    KeySpecs = [{port, [{typecast, fun parse_port/1}]},
                {address, [{typecast, fun parse_addr/1}]}],
    FlagSpecs = [],
    Callback =
    fun(_, [{address, Addr},{port, Port}], []) ->
            restart_listener(Addr, Port);
       (_, [{port, Port}, {address, Addr}], []) ->
            restart_listener(Addr, Port);
       (_, _, _) ->
            Text = clique_status:text(vmq_listener_restart_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

restart_listener(Addr, Port) ->
    case vmq_ranch_config:restart_listener(Addr, Port) of
        ok ->
            [clique_status:text("Done")];
        {error, Reason} ->
            Text = io_lib:format("can't restart listener due to '~p'", [Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end.

vmq_listener_show_cmd() ->
    Cmd = ["vmq-admin", "listener", "show"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback =
    fun(_, [], []) ->
            Table =
            lists:foldl(
              fun({Type, Ip, Port, Status, MP, MaxConns}, Acc) ->
                      [[{type, Type}, {status, Status}, {ip, Ip},
                        {port, Port}, {mountpoint, MP}, {max_conns, MaxConns}]
                       |Acc]
              end, [], vmq_ranch_config:listeners()),
              [clique_status:table(Table)]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

cleanup_flags([{K, undefined}|Rest], Acc) ->
    %% e.g. from --websocket or --ssl
    cleanup_flags(Rest, [{K, true}|Acc]);
cleanup_flags([KV|Rest], Acc) ->
    cleanup_flags(Rest, [KV|Acc]);
cleanup_flags([], Acc) -> Acc.

parse_port(StrP) ->
    case catch list_to_integer(StrP) of
        P when (P >= 0) and (P=<65535) -> P;
        _ -> {error, {invalid_args,[{port, StrP}]}}
    end.

parse_addr(StrA) ->
    case string:split(StrA, ":") of
        ["local", FS] -> {local, FS};
        _ -> case inet:parse_address(StrA) of
            {ok, Ip} -> Ip;
            {error, einval} ->
                {error, {invalid_args,[{address, StrA}]}}
        end
    end.

vmq_listener_usage() ->
    ["vmq-admin listener <sub-command>\n\n",
     "  starts, modifies, and stops listeners.\n\n",
     "  Sub-commands:\n",
     "    start       Create and start a listener\n",
     "    stop        Stop accepting new connections for a running listener\n",
     "    restart     Restart accepting new connections for a stopped listener\n",
     "    delete      Delete a stopped listener\n",
     "    show        Show all listeners\n",
     "  Use --help after a sub-command for more details.\n"
    ].

vmq_listener_start_usage() ->
    ["vmq-admin listener start address=IpAddr port=Port\n\n",
     "  Creates and starts a new listener.\n\n",
     "General Options\n\n",
     "  -m, --mountpoint=Mountpoint\n",
     "  --nr_of_acceptors=NrOfAcceptors\n",
     "  --max_connections=[infinity | MaxConnections]\n",
     "  --allowed_protocol_versions=[3|4|5]\n",
     "      Defaults to 3,4\n\n",
     "WebSocket Options\n\n",
     "  --websocket\n",
     "      use the Websocket protocol as the underlying transport\n\n",
     "PROXYv2 Options\n\n",
     "  --proxy_protocol\n",
     "      Enable PROXY v2 protocol for this listener\n",
     "  --use_cn_as_username\n",
     "      If PROXY v2 is enabled for this listener use this flag\n",
     "      to decide if the common name should replace the MQTT username\n",
     "      Enabled by default (use `=false`) to disable\n\n",
     "SSL Options\n\n",
     "  --ssl\n",
     "      use SSL for this listener, without this option, all other SSL\n",
     "      are ignored\n",
     "  --cafile=CaFile\n",
     "      The path to the cafile containing the PEM encoded CA certificates\n" ,
     "      that are trusted by the server.\n",
     "  --certfile=CertificateFile\n",
     "      The path to the PEM encoded server certificate\n",
     "  --keyfile=KeyFile\n",
     "      The path to the PEM encoded key file\n",
     "  --ciphers=CiphersList\n",
     "      The list of allowed ciphers, each separated by a colon\n",
     "  --eccs=EllipticCurveList\n",
     "      The list of allowed elliptic curves, each separated by a colon\n",
     "  --crlfile=CRLFile\n",
     "      If --require-certificate is set, you can use a certificate\n",
     "      revocation list file to revoke access to particular client\n",
     "      certificates. The file has to be PEM encoded.\n",
     "  --tls_version=TLSVersion\n",
     "      use this TLS version for the listener\n",
     "  --require_certificate\n",
     "      Use client certificates to authenticate your clients\n",
     "  --use_identity_as_username\n",
     "      If --require-certificate is set, the CN value from the client\n",
     "      certificate is used as the username for authentication\n\n"
    ].

vmq_listener_stop_usage() ->
    ["vmq-admin listener stop address=IpAddr port=Port\n\n",
     "  Stops accepting new connections on a running listener.\n\n",
     "Options\n\n",
     "  -k, --kill_sessions\n"
     "      kills all sessions accepted with this listener.\n\n"
    ].

vmq_listener_delete_usage() ->
    ["vmq-admin listener delete address=IpAddr port=Port\n\n",
     "  Deletes a stopped listener.\n\n"
    ].

vmq_listener_restart_usage() ->
    ["vmq-admin listener restart address=IpAddr port=Port\n\n",
     "  Restarts accepting new connections for a stopped listener.\n\n"
    ].
