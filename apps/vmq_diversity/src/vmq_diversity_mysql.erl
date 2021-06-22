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

-module(vmq_diversity_mysql).
-include_lib("emysql/include/emysql.hrl").
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"execute">>, #erl_func{code=fun execute/2}},
     {<<"ensure_pool">>, #erl_func{code=fun ensure_pool/2}},
     {<<"hash_method">>, #erl_func{code=fun hash_method/2}}
    ].

execute(As, St) ->
    case As of
        [BPoolId, BQuery|Args] when is_binary(BPoolId)
                                    and is_binary(BQuery) ->
            PoolId =
            try list_to_existing_atom(binary_to_list(BPoolId)) of
                APoolId -> APoolId
            catch
                _:_ ->
                    lager:error("unknown pool ~p", [BPoolId]),
                    badarg_error(unknown_pool, As, St)
            end,
            try emysql:execute(PoolId, BQuery, Args) of
                #result_packet{} = Result ->
                    {Table, NewSt} = luerl:encode(emysql:as_proplist(Result), St),
                    {[Table], NewSt};
                #ok_packet{} ->
                    {[true], St};
                #error_packet{} ->
                    {[false], St};
                #eof_packet{} ->
                    {[false], St}
            catch
                E:R ->
                    lager:error("can't execute query ~p due to ~p:~p", [BQuery, E, R]),
                    badarg_error(execute_equery, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

ensure_pool(As, St) ->
    case As of
        [Config0|_] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    Options = vmq_diversity_utils:map(Config),
                    {ok, AuthConfigs} = application:get_env(vmq_diversity, db_config),
                    DefaultConf = proplists:get_value(mysql, AuthConfigs),
                    PoolId = vmq_diversity_utils:atom(
                               maps:get(<<"pool_id">>,
                                        Options,
                                        pool_mysql)),

                    Size = vmq_diversity_utils:int(
                             maps:get(<<"size">>,
                                      Options,
                                      proplists:get_value(pool_size, DefaultConf))),
                    User = vmq_diversity_utils:str(
                             maps:get(<<"user">>,
                                      Options,
                                      proplists:get_value(user, DefaultConf))),
                    Password = vmq_diversity_utils:str(
                                 maps:get(<<"password">>,
                                          Options,
                                          proplists:get_value(password, DefaultConf))),
                    Host = vmq_diversity_utils:str(
                             maps:get(<<"host">>,
                                      Options,
                                      proplists:get_value(host, DefaultConf))),
                    Port = vmq_diversity_utils:int(
                             maps:get(<<"port">>, Options,
                                      proplists:get_value(port, DefaultConf))),
                    Database = vmq_diversity_utils:ustr(
                                 maps:get(<<"database">>,
                                          Options,
                                          proplists:get_value(database, DefaultConf))),
                    Encoding = vmq_diversity_utils:atom(
                                 maps:get(<<"encoding">>,
                                          Options,
                                          proplists:get_value(encoding, DefaultConf))),
                    NewOptions =
                    [{size, Size}, {user, User}, {password, Password},
                     {host, Host}, {port, Port}, {database, Database},
                     {encoding, Encoding}],
                    vmq_diversity_sup:start_all_pools(
                      [{mysql, [{id, PoolId}, {opts, NewOptions}]}], []),

                    % return to lua
                    {[true], St};
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

hash_method(_, St) ->
    {ok, DBConfigs} = application:get_env(vmq_diversity, db_config),
    DefaultConf = proplists:get_value(mysql, DBConfigs),
    HashMethod = proplists:get_value(password_hash_method, DefaultConf),
    MysqlFunc = case HashMethod of
        password -> <<"PASSWORD(?)">>;
        md5 -> <<"MD5(?)">>;
        sha1 -> <<"SHA1(?)">>;
        sha256 -> <<"SHA2(?, 256)">>;
        sha1_salt -> <<"SHA1(CONCAT(?, salt))">>;
        sha256_salt -> <<"SHA2(CONCAT(?, salt), 256)">>
    end, 
    {[MysqlFunc], St}.
