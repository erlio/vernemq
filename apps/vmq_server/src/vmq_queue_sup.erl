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

-module(vmq_queue_sup).

%% API functions
-export([start_link/5,
         start_queue/3,
         get_queue_pid/2,
         fold_queues/3,
         nr_of_queues/1]).

%% Supervisor callbacks
-export([init/5]).
-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-record(state, {parent, shutdown, r=0, max_r, max_t, reset_timer, queue_tab}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Shutdown, RegName, QueueTabId, MaxR, MaxT) ->
    case proc_lib:start_link(?MODULE, init, [self(), Shutdown, QueueTabId, MaxR, MaxT * 1000]) of
        {ok, Pid} = Ret ->
            register(RegName, Pid),
            Ret;
        {error, Error} ->
            {error, Error}
    end.


start_queue(SupPid, SubscriberId, Clean) ->
    %% The Clean flag is used to distinguish between Queues created during
    %% broker start and newly created ones.
    %% The main difference is that the ones created at broker start have
    %% to initialize themselves using the message store,
    %% whereas new ones don't require to access the message store at startup
    %%
    %% if a queue terminates abnormally and gets restarted by this supervisor
    %% we enforce the roundtrip to the message store.
    Ref = make_ref(),
    SupPid ! {?MODULE, {self(), Ref}, {start_queue, SubscriberId, Clean}},
    receive
        {Ref, Reply} -> Reply
    end.

get_queue_pid(QueueTabId, SubscriberId) ->
    case catch ets:lookup(QueueTabId, SubscriberId) of
        {'EXIT', {badarg,Reason}} ->
            %% handle exception to avoid crash !
            lager:warning("queue_sup:get_queue_pid intercepted {badarg, ~p} for qtabid ~p subscriberid ~p", [Reason, QueueTabId, SubscriberId]),
            not_found;
        [] ->
            not_found;
        [{_, Pid}] ->
            Pid
    end.

fold_queues(QueueTabId, FoldFun, Acc) ->
    ets:foldl(fun({SubscriberId, QPid}, AccAcc) ->
                      FoldFun(SubscriberId, QPid, AccAcc)
              end, Acc, QueueTabId).

nr_of_queues(QueueTabId) ->
    case ets:info(QueueTabId, size) of
        undefined -> 0;
        V -> V
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(Parent, Shutdown, QueueTabId, MaxR, MaxT) ->
    process_flag(trap_exit, true),
    ok = proc_lib:init_ack(Parent, {ok, self()}),
    ets:new(QueueTabId, [public, {read_concurrency, true}, named_table]),
    loop(#state{parent=Parent, shutdown=Shutdown, max_r=MaxR, max_t=MaxT, queue_tab = QueueTabId}, 0).

loop(State = #state{parent=Parent, queue_tab = QueueTab}, NrOfChildren) ->
    receive
        {?MODULE, Caller, {start_queue, SubscriberId, Clean}} ->
            case ets:lookup(QueueTab, SubscriberId) of
                [] ->
                    loop(State, start_queue(Caller, SubscriberId, Clean, NrOfChildren, QueueTab));
                [{_, Pid}] ->
                    %% already started
                    reply(Caller, {ok, true, Pid}),
                    loop(State, NrOfChildren)
            end;
        {'EXIT', Parent, Reason} ->
            terminate(State, NrOfChildren, Reason);
        {'EXIT', Pid, Reason} ->
            erase(Pid),
            case Reason of
                shutdown ->
                    ets:match_delete(QueueTab, {'_', Pid}),
                    loop(State, NrOfChildren - 1);
                normal ->
                    ets:match_delete(QueueTab, {'_', Pid}),
                    loop(State, NrOfChildren - 1);
                Reason ->
                    #state{r=R, max_r=MaxR, max_t=MaxT, reset_timer=Reset} = State,
                    case R >= MaxR of
                        true ->
                            terminate(State#state{shutdown=brutal_kill},
                                      NrOfChildren, exhausted_restart_strategy);
                        false ->
                            [{SubscriberId, _}] = ets:match_object(QueueTab, {'_', Pid}),
                            report_error(SubscriberId, Pid, Reason),
                            loop(State#state{r=R + 1,
                                             reset_timer=maybe_set_reset_timer(MaxT, Reset)},
                                 start_queue(undefined, SubscriberId, false, NrOfChildren - 1, QueueTab))
                    end
            end;
        {?MODULE, reset_timer} ->
            loop(State#state{r=0, reset_timer=undefined}, NrOfChildren);
        {system, From, Request} ->
			sys:handle_system_msg(Request, From, Parent, ?MODULE, [],
                                  {State, NrOfChildren});
        %% Calls from the supervisor module.
        {'$gen_call', {To, Tag}, which_children} ->
            Children =
                ets:foldl(
                  fun({_,Pid}, Acc) ->
                          [{vmq_queue, Pid, worker, [vmq_queue]} | Acc]
                  end,
                  [],
                  QueueTab),
            To ! {Tag, Children},
            loop(State, NrOfChildren);
        {'$gen_call', {To, Tag}, count_children} ->
            Counts =
                [{specs, 1}, {active, NrOfChildren},
                 {supervisors, 0}, {workers, NrOfChildren}],
            To ! {Tag, Counts},
            loop(State, NrOfChildren);
        {'$gen_call', {To, Tag}, _} ->
            To ! {Tag, {error, ?MODULE}},
            loop(State, NrOfChildren);
        Msg ->
            lager:error("vmq_queue_sup received unexpected message ~p", [Msg])
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Kill all children and then exit. We unlink first to avoid
%% getting a message for each child getting killed.
terminate(#state{shutdown=brutal_kill, queue_tab = QueueTab}, _, Reason) ->
    ets:foldl(
      fun({_,Pid}, Acc) ->
              unlink(Pid),
              exit(Pid, kill),
              Acc
      end,
      [],
      QueueTab),
    exit(Reason);
%% Attemsupervisor.htmlpt to gracefully shutdown all children.
terminate(#state{shutdown=Shutdown, queue_tab = QueueTab}, NrOfChildren, Reason) ->
    shutdown_children(QueueTab),
    case Shutdown of
        infinity ->
            ok;
        _ ->
            erlang:send_after(Shutdown, self(), kill)
    end,
    wait_children(NrOfChildren, QueueTab),
    ets:delete_all_objects(QueueTab),
    exit(Reason).

shutdown_children(QueueTab) ->
    ets:foldl(
      fun({_,Pid}, Acc) ->
              monitor(process, Pid),
              unlink(Pid),
              exit(Pid, shutdown),
              Acc
      end,
      [],
      QueueTab),
    ok.

wait_children(0, _Tab) -> ok;
wait_children(NrOfChildren, Tab) ->
    receive
        {'DOWN', _, process, Pid, _} ->
            ets:match_delete(Tab, {'_', Pid}),
            wait_children(NrOfChildren - 1, Tab);
        kill ->
            ets:foldl(
              fun({_,Pid}, Acc) ->
                      exit(Pid, kill),
                      Acc
              end,
              [],
              Tab),
            ok
    end.

start_queue(Caller, SubscriberId, Clean, NrOfChildren, QueueTab) ->
    try vmq_queue:start_link(SubscriberId, Clean) of
        {ok, Pid} ->
            ets:insert(QueueTab, {SubscriberId, Pid}),
            reply(Caller, {ok, false, Pid}),
            NrOfChildren + 1;
        Ret ->
            lager:error("vmq_queue_sup can't start vmq_queue for ~p due to ~p",
                        [SubscriberId, Ret]),
            reply(Caller, {error, cant_start_queue}),
            NrOfChildren
    catch
        Class:Reason ->
            lager:error("vmq_queue_sup can't start vmq_queue for ~p due crash ~p:~p",
                        [SubscriberId, Class, Reason]),
            reply(Caller, {error, Reason}),
            NrOfChildren
    end.

maybe_set_reset_timer(MaxT, undefined) ->
    erlang:send_after(MaxT, self(), {?MODULE, reset_timer});
maybe_set_reset_timer(_, TRef) -> TRef.

reply({CallerPid, CallerRef}, Reply) ->
    CallerPid ! {CallerRef, Reply};
reply(undefined, Reply) -> Reply.

report_error(SubscriberID, Pid, Reason) ->
    lager:error("vmq_queue process ~p exit for subscriber ~p due to ~p",
                [Pid, SubscriberID, Reason]).

system_continue(_, _, {State, NrOfChildren}) ->
	loop(State, NrOfChildren).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, {State, NrOfChildren}) ->
	terminate(State, NrOfChildren, Reason).

system_code_change(Misc, _, _, _) ->
	{ok, Misc}.
