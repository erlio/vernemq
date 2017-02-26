%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_diversity_mongo).

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).


insert(PoolName, Collection, DocOrDocs) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:insert(Worker, Collection, DocOrDocs)
                                  end).

update(PoolName, Collection, Selector, Command) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:update(Worker, Collection, Selector, Command)
                                  end).

delete(PoolName, Collection, Selector) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:delete(Worker, Collection, Selector)
                                  end).

find(PoolName, Collection, Selector, Args) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:find(Worker, Collection, Selector, Args)
                                  end).

find_one(PoolName, Collection, Selector, Args) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:find_one(Worker, Collection, Selector, Args)
                                  end).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"ensure_pool">>, {function, fun ensure_pool/2}},
     {<<"insert">>, {function, fun insert/2}},
     {<<"update">>, {function, fun update/2}},
     {<<"delete">>, {function, fun delete/2}},
     {<<"find">>, {function, fun find/2}},
     {<<"find_one">>, {function, fun find_one/2}},
     {<<"next">>, {function, fun cursor_next/2}},
     {<<"take">>, {function, fun cursor_take/2}},
     {<<"close">>, {function, fun cursor_close/2}}
    ].

ensure_pool(As, St) ->
    case As of
        [Config0|_] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    {ok, AuthConfigs} = application:get_env(vmq_diversity, db_config),
                    DefaultConf = proplists:get_value(mongodb, AuthConfigs),
                    Options = vmq_diversity_utils:map(Config),
                    PoolId = vmq_diversity_utils:atom(
                               maps:get(<<"pool_id">>,
                                        Options,
                                        pool_mongodb)),

                    Size = vmq_diversity_utils:int(
                             maps:get(<<"size">>,
                                      Options,
                                      proplists:get_value(pool_size, DefaultConf))),
                    MaxOverflow =
                    vmq_diversity_utils:int(
                      maps:get(<<"max_overflow">>,
                               Options, 20)),
                    Login = vmq_diversity_utils:ustr(
                              maps:get(<<"login">>,
                                       Options,
                                       proplists:get_value(login, DefaultConf))),
                    Password = vmq_diversity_utils:ustr(
                                 maps:get(<<"password">>,
                                          Options,
                                          proplists:get_value(password, DefaultConf))),
                    Host = vmq_diversity_utils:str(
                             maps:get(<<"host">>,
                                      Options,
                                      proplists:get_value(host, DefaultConf))),
                    Port = vmq_diversity_utils:int(
                             maps:get(<<"port">>,
                                      Options,
                                      proplists:get_value(port, DefaultConf))),
                    Database = vmq_diversity_utils:ustr(
                                 maps:get(<<"database">>,
                                          Options,
                                          proplists:get_value(database, DefaultConf))),
                    WMode = vmq_diversity_utils:atom(
                              maps:get(<<"w_mode">>,
                                       Options,
                                       proplists:get_value(w_mode, DefaultConf))),
                    RMode = vmq_diversity_utils:atom(
                              maps:get(<<"r_mode">>,
                                       Options,
                                       proplists:get_value(r_mode, DefaultConf))),
                    NewOptions =
                    [{login, mbin(Login)}, {password, mbin(Password)},
                     {host, Host}, {port, Port}, {database, mbin(Database)},
                     {w_mode, WMode}, {r_mode, RMode}],
                    vmq_diversity_sup:start_pool(mongodb,
                                                 [{id, PoolId},
                                                  {size, Size},
                                                  {max_overflow, MaxOverflow},
                                                  {opts, NewOptions}]),

                    % return to lua
                    {[true], St};
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.
mbin(undefined) -> undefined;
mbin(Str) when is_list(Str) -> list_to_binary(Str).

insert(As, St) ->
    case As of
        [BPoolId, Collection, DocOrDocs] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(DocOrDocs, St) of
                [{K, V}|_] = TableOrTables when is_binary(K)
                                                or (is_integer(K) and is_list(V)) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try insert(PoolId, Collection,
                                check_ids(vmq_diversity_utils:map(TableOrTables))) of
                        Result1 ->
                            {Result2, NewSt} = luerl:encode(
                                                 vmq_diversity_utils:unmap(check_ids(Result1)), St),
                            {[Result2], NewSt}
                    catch
                        E:R ->
                            lager:error("can't execute insert ~p due to ~p:~p",
                                        [TableOrTables, E, R]),
                            badarg_error(execute_insert, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

update(As, St) ->
    case As of
        [BPoolId, Collection, Selector0, Doc] when is_binary(BPoolId)
                                                       and is_binary(Collection) ->
            case {luerl:decode(Selector0, St),
                  luerl:decode(Doc, St)} of
                {[{SelectKey, _}|_] = Selector, [{K,_}|_] = Command}
                  when is_binary(SelectKey) and is_binary(K) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try update(PoolId, Collection, check_ids(vmq_diversity_utils:map(Selector)),
                               #{<<"$set">> => check_ids(vmq_diversity_utils:map(Command))}) of
                        ok ->
                            {[true], St}
                    catch
                        E:R ->
                            lager:error("can't execute update ~p due to ~p:~p",
                                        [{Selector, Command}, E, R]),
                            badarg_error(execute_update, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

delete(As, St) ->
    case As of
        [BPoolId, Collection, Selector0] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(Selector0, St) of
                Selector when is_list(Selector) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try delete(PoolId, Collection, check_ids(vmq_diversity_utils:map(Selector))) of
                        ok ->
                            {[true], St}
                    catch
                        E:R ->
                            lager:error("can't execute delete ~p due to ~p:~p",
                                        [Selector, E, R]),
                            badarg_error(execute_update, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

find(As, St) ->
    case As of
        [BPoolId, Collection, Selector0|Args] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(Selector0, St) of
                Selector when is_list(Selector) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try find(PoolId, Collection, check_ids(vmq_diversity_utils:map(Selector)), parse_args(Args, St)) of
                        CursorPid when is_pid(CursorPid) ->
                            %% we re passing the cursor pid as a binary to the Lua Script
                            BinPid = list_to_binary(pid_to_list(CursorPid)),
                            {[<<"mongo-cursor-", BinPid/binary>>], St}
                    catch
                        E:R ->
                            lager:error("can't execute find ~p due to ~p:~p",
                                        [Selector, E, R]),
                            badarg_error(execute_find, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

find_one(As, St) ->
    case As of
        [BPoolId, Collection, Selector0|Args] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(Selector0, St) of
                Selector when is_list(Selector) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try find_one(PoolId, Collection, check_ids(vmq_diversity_utils:map(Selector)), parse_args(Args, St)) of
                        Result1 when map_size(Result1) > 0 ->
                            {Result2, NewSt} = luerl:encode(
                                                 vmq_diversity_utils:unmap(check_ids(Result1)), St),
                            {[Result2], NewSt};
                        _ ->
                            {[false], St}
                    catch
                        E:R ->
                            lager:error("can't execute find_one ~p due to ~p:~p",
                                        [Selector, E, R]),
                            badarg_error(execute_find_one, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.


cursor_next(As, St) ->
    case As of
        [<<"mongo-cursor-", BinPid/binary>>] ->
            CursorPid = list_to_pid(binary_to_list(BinPid)),
            case mc_cursor:next(CursorPid) of
                error ->
                    {[false], St};
                {Result1} ->
                    {Result2, NewSt} = luerl:encode(vmq_diversity_utils:unmap(check_ids(Result1)), St),
                    {[Result2], NewSt}
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

cursor_take(As, St) ->
    case As of
        [<<"mongo-cursor-", BinPid/binary>>, N] when is_number(N) ->
            CursorPid = list_to_pid(binary_to_list(BinPid)),
            case mc_cursor:take(CursorPid, round(N)) of
                error ->
                    {[false], St};
                Result1 ->
                    {Result2, NewSt} = luerl:encode(vmq_diversity_utils:unmap(check_ids(Result1)), St),
                    {[Result2], NewSt}
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.


cursor_close(As, St) ->
    case As of
        [<<"mongo-cursor-", BinPid/binary>>] ->
            CursorPid = list_to_pid(binary_to_list(BinPid)),
            mc_cursor:close(CursorPid),
            {[true], St};
        _ ->
            badarg_error(execute_parse, As, St)
    end.

pool_id(BPoolId, As, St) ->
    try list_to_existing_atom(binary_to_list(BPoolId)) of
        APoolId -> APoolId
    catch
        _:_ ->
            lager:error("unknown pool ~p", [BPoolId]),
            badarg_error(unknown_pool, As, St)
    end.

parse_args([], _) -> [];
parse_args([Table], St) when is_tuple(Table) ->
    case luerl:decode(Table, St) of
        [{K, _}|_] = Args when is_binary(K) ->
            parse_args_(Args, []);
        _ ->
            []
    end;
parse_args(_, _) ->
    % everything else gets ignored
    [].

parse_args_([{K, V}|Rest], Acc) ->
    try list_to_existing_atom(binary_to_list(K)) of
        AK when is_list(V) ->
            parse_args_(Rest, [{AK, vmq_diversity_utils:map(V)}|Acc]);
        AK ->
            parse_args_(Rest, [{AK, V}|Acc])
    catch
        _:_ ->
            parse_args_(Rest, Acc)
    end;
parse_args_([], Acc) -> Acc.

%% Remaps the MongoDB ObjectIds to a structure readable by Lua.
%% The ObjectId is autogenerated by the MongoDB driver in case
%% no _id element is given in the document.
check_ids(Doc) when is_map(Doc) -> check_id(Doc);
check_ids(Docs) when is_list(Docs) -> check_ids(Docs, []).

check_ids([], Acc) -> lists:reverse(Acc);
check_ids([Doc|Rest], Acc) ->
    check_ids(Rest, [check_id(Doc)|Acc]).

check_id(#{<<"_id">> := {ObjectId}} = Doc) ->
    Doc#{<<"_id">> => <<"vmq-objid", ObjectId/binary>>};
check_id(#{<<"_id">> := <<"vmq-objid", ObjectId/binary>>} = Doc) ->
    Doc#{<<"_id">> => {ObjectId}};
check_id(Doc) -> Doc. %% empty doc
