%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_db_rocksdb).
-include("vmq_swc.hrl").
-behaviour(vmq_swc_db).
-behaviour(gen_server).

%for vmq_swc_db behaviour
-export([childspecs/2,
         write/3,
         read/4,
         iterator/4,
         iterator_next/1,
         iterator_close/1]).

-export([start_link/2,
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {name, db, data_root, read_opts, write_opts, open_opts}).
-record(db, {handle, default, dcc, log}).

% vmq_swc_db impl
childspecs(#swc_config{group=SwcGroup} = Config, Opts) ->
    [#{id => {?MODULE, SwcGroup},
       start => {?MODULE, start_link, [Config, Opts]}}].

-spec write(config(), list(kv()), opts()) -> ok.
write(#swc_config{db=DBName}, Objects, Opts) ->
    gen_server:call(DBName, {write, Objects, Opts}, infinity).

-spec read(config(), type(), key(), opts()) -> {ok, value()} | not_found.
read(#swc_config{db=DBName}, Type, Key, Opts) ->
    [{_, #db{handle=Handle} = DB}] = ets:lookup(DBName, refs),
    CF = db_cf(Type, DB),
    rocksdb:get(Handle, CF, Key, Opts).

-spec iterator(config(), type(), key() | first, opts()) -> any().
iterator(#swc_config{db=DBName}, Type, FirstKey, Opts) ->
    [{_, #db{handle=Handle} = DB}] = ets:lookup(DBName, refs),
    CF = db_cf(Type, DB),
    {ok, Snapshot} = rocksdb:snapshot(Handle),
    {ok, Iterator} = rocksdb:iterator(Handle, CF, [{snapshot, Snapshot}|Opts]),
    {FirstKey, Iterator, Snapshot}.

-spec iterator_next(any()) -> {{key(), value()}, any()} | '$end_of_table'.
iterator_next({NextItrAction, Iterator, Snapshot}) ->
    case rocksdb:iterator_move(Iterator, NextItrAction) of
        {ok, Key, Val} ->
            {{Key, Val}, {next, Iterator, Snapshot}};
        {error, _} ->
            % iterator is already closed at this point, release snapshot
            rocksdb:release_snapshot(Snapshot),
            '$end_of_table'
    end.

-spec iterator_close(any()) -> ok.
iterator_close({_, Iterator, Snapshot}) ->
    try
        rocksdb:iterator_close(Iterator),
        rocksdb:release_snapshot(Snapshot)
    catch
        _:_ ->
            ok
    end,
    ok.

%% gen_server impl
start_link(#swc_config{db=DBName} = Config, Opts) ->
    gen_server:start_link({local, DBName}, ?MODULE, [Config | Opts], []).

init([#swc_config{peer=Peer, group=SwcGroup, db=DBName} = _Config|Opts]) ->
    DefaultDataDir = filename:join(filename:join(<<".">>, Peer), SwcGroup),

    DataDir = proplists:get_value(data_dir, Opts,
                                  application:get_env(vmq_swc, data_dir, binary_to_list(DefaultDataDir))),
    DbPath = filename:absname(DataDir),
    filelib:ensure_dir(DbPath),

    CreateIfMissing = proplists:get_value(create_if_missing, Opts, true),
    CreateMissingCF = proplists:get_value(create_missing_column_families, Opts, true),
    % TODO Support further Rocksdb opts

    ReadOpts = proplists:get_value(read_opts, Opts, []),
    WriteOpts = proplists:get_value(write_opts, Opts, []),
    OpenOpts = [{create_if_missing, CreateIfMissing},
                {create_missing_column_families, CreateMissingCF}],

    process_flag(trap_exit, true),

    ets:new(DBName, [named_table, public, {read_concurrency, true}]),

    State0 = #state{name=DBName, data_root=DbPath, read_opts=ReadOpts, write_opts=WriteOpts, open_opts=OpenOpts},

    open_db(Opts, State0).

handle_call({write, Objects, Opts}, _From, #state{db=#db{handle=DbHandle} = DB} = State) ->
    DBOps =
    lists:map(fun({Type, Key, ?DELETED}) ->
                      CF = db_cf(Type, DB),
                      {delete, CF, Key};
                 ({Type, Key, Value}) ->
                      CF = db_cf(Type, DB),
                      {put, CF, Key, Value}
              end, Objects),
    rocksdb:write(DbHandle, DBOps, Opts),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db=#db{handle=DbHandle}} =_State) ->
    catch rocksdb:close(DbHandle),
    ok.

code_change(_OldVsn, _NewVsn, State) ->
    State.

db_cf(default, #db{default=CF}) -> CF;
db_cf(dcc, #db{dcc=CF}) -> CF;
db_cf(log, #db{log=CF}) -> CF.

open_db(Opts, State) ->
    RetriesLeft = proplists:get_value(open_retries, Opts, 30),
    open_db(Opts, State, max(1, RetriesLeft), undefined).

open_db(_Opts, _State0, 0, LastError) ->
    {error, LastError};
open_db(Opts, State0, RetriesLeft, _) ->
    ColumnFamilies = [{"default", []}, {"dcc", []}, {"log", []}],
    case rocksdb:open_with_cf(State0#state.data_root, State0#state.open_opts, ColumnFamilies) of
        {ok, Ref, [Default_CF, DCC_CF, Log_CF]} ->
            DBHandle = #db{handle=Ref,
                           dcc = DCC_CF,
                           log = Log_CF,
                           default = Default_CF},
            ets:insert(State0#state.name, {refs, DBHandle}),

            {ok, State0#state { db = DBHandle }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed instance takes some time to flush rocksdb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: While lock ", OpenErr) of
                true ->
                    SleepFor = proplists:get_value(open_retry_delay, Opts, 2000),
                    lager:debug("VerneMQ RocksDB backend retrying ~p in ~p ms after error ~s\n",
                                [State0#state.data_root, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(Opts, State0, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

