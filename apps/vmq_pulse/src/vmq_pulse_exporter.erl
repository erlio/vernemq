%% Copyright 2019 Octavo Labs AG Switzerland (http://octavolabs.com)
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
-module(vmq_pulse_exporter).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(PULSE_VERSION, 1).
-define(PULSE_USER, "vmq-pulse-1").

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    self() ! push,
    _ = cpu_sup:util(), % first value is garbage
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(push, State) ->
    ClusterId = vmq_pulse:get_cluster_id(),
    {ok, PulseUrl} = application:get_env(vmq_pulse, url),
    {ok, Interval} = application:get_env(vmq_pulse, push_interval),
    ApiKey = application:get_env(vmq_pulse, api_key, undefined),
    NodeId = atom_to_binary(node(), utf8),
    case (ClusterId =/= undefined) of
        true ->
            Body = create_body(ClusterId, NodeId),
            try push_body(PulseUrl, ApiKey, Body) of
                ok -> ok;
                {error, Reason} ->
                    error_logger:warning_msg("can't push pulse due to error ~p", [Reason])
            catch
                E:R ->
                    error_logger:warning_msg("can't push pulse due to unknown error ~p ~p", [E, R])
            end;
        false ->
            ignore
    end,
    erlang:send_after(interval(Interval), self(), push),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
create_body(ClusterId, Node) ->
    Body =
    [
        {node, Node},
        {cluster_id, ClusterId},
        {pulse_version, ?PULSE_VERSION},
        {plugins, vmq_plugin:info(all)},
        {metrics, lists:filtermap(
                    fun({{metric_def, Type, Labels, _Id, Name, Desc}, Val}) ->
                            {true, #{type => Type,
                                     labels => Labels,
                                     name => Name,
                                     desc => Desc,
                                     value => Val}};
                       (_UnsupportedMetricDef) ->
                            false
                    end, vmq_metrics:metrics(#{aggregate => false}))},
        {cpu_util, cpu_sup:util()},
        {mem_util, memsup:get_system_memory_data()},
        {disk_util, disk_util()},
        {applications, [{App, Vsn} || {App, _, Vsn} <- application:which_applications()]},
        {system_version, erlang:system_info(system_version)},
        {cluster_status, vmq_cluster:status()},
        {uname, os:cmd("uname -a")}
    ],
    zlib:compress(term_to_binary(Body)).

push_body(PulseUrl, ApiKey, Body) ->
    ContentType = "application/x-vmq-pulse-1",
    Headers = case ApiKey of
                  undefined -> [];
                  _ ->
                      AuthStr = base64:encode_to_string(?PULSE_USER ++ ":" ++ ApiKey),
                      [{"Authorization", "Basic " ++ AuthStr}]
              end,
    {ok, ConnectTimeout} = application:get_env(vmq_pulse, connect_timeout),
    HTTPOpts =  [{timeout, 60000},
                 {connect_timeout, ConnectTimeout * 1000},
                 {autoredirect, true}],

    case httpc:request(post, {PulseUrl, Headers, ContentType, Body}, HTTPOpts, []) of
        {ok, _} ->
            ok;
        {error, _Reason} = E ->
            E
    end.

interval(Int) when Int < 10 -> 10 * 1000;
interval(Int) -> Int * 1000.

disk_util() ->
    {ok, MsgStoreOpts} = application:get_env(vmq_server, msg_store_opts),
    MsgStoreDisk = find_disk_for_directory(proplists:get_value(store_dir, MsgStoreOpts)),
    MetadataDisk =
    case metadata_backend() of
        vmq_swc ->
            {ok, SWCDataDir} = application:get_env(vmq_swc, data_dir),
            find_disk_for_directory(SWCDataDir);
        vmq_plumtree ->
            {ok, PlumtreeDataDir} = application:get_env(plumtree, plumtree_data_dir),
            find_disk_for_directory(PlumtreeDataDir)
    end,
    [{msg_store, MsgStoreDisk}, {meta, MetadataDisk}].

find_disk_for_directory(Directory) when is_list(Directory) ->
    find_disk_for_directory(list_to_binary(Directory));
find_disk_for_directory(Directory) when is_binary(Directory) ->
    {_, Disk} =
    lists:foldl(
      fun({Mount, _, _} = Disk, {PrefixSize, _} = Acc) ->
              case binary:longest_common_prefix([list_to_binary(Mount), Directory]) of
                  NewPrefixSize when NewPrefixSize > PrefixSize ->
                      {NewPrefixSize, Disk};
                  _ ->
                      Acc
              end;
         (_, Acc) ->
              Acc
      end, {0, no_disk}, lists:keysort(1, disksup:get_disk_data())),
    Disk;
find_disk_for_directory(_) -> no_disk.

metadata_backend() ->
    {ok, MetadataBackend} = application:get_env(vmq_server, metadata_impl),
    MetadataBackend.
