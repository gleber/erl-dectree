%% Author: gleber
%% Created: May 17, 2007
-module(data_server).

%%
%% Include files
%%

-import(log, [log/1, log/2]).

-define(TIMEOUT, 5000).

%%
%% Exported Functions
%%
-export([new/0,
	 init/3,
	 stop/0,
	 local_create_tree/0,
	 get_name/1,
	 get_type/1,
	 get_types/0,
	 get_columns/0,
	 get_dep_column/0,
	 get_indep_columns/0,
	 set_child_data/1,
	 get_child/1,
	 gather_data/3,
	 distributed_create_tree/0    
	]).

-export([data_worker/1, tree_constructor/1]). 

%%
%% API Functions
%%


init(Names, Types, Deps) ->
    Nodes = [node() | nodes()],
    global:send(data_server, {init_data, Names, Types, Deps, Nodes}).

stop() ->
    case global:whereis_name(data_server) of
	undefined ->
	    false;
	_ ->
	    global:send(data_server, {exit, self()}),
	    receive
		exitted ->
		    ok
	    after ?TIMEOUT ->
		    erlang:error(timeout)
	    end
    end.

claim_children(Children) ->
    global:send(data_server, {claim_children, self(), Children}),
    receive
	{reject, Idle} ->
	    {reject, Idle};
	{accept, C} ->
	    {accept, C}
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_type(Col) ->
    global:send(data_server, {get_type, self(), Col}),
    receive
	{type, Col, Type} ->
	    Type	    
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_types() ->
    global:send(data_server, {get_types, self()}),
    receive
	{types, Types} ->
	    Types	    
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_name(Col) ->
    global:send(data_server, {get_name, self(), Col}),
    receive
	{name, Col, Name} ->
	    Name	    
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_columns() ->
    global:send(data_server, {get_columns, self()}),
    receive
	{columns, Col} ->
	    Col	    
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_indep_columns() ->
    global:send(data_server, {get_indep_columns, self()}),
    receive
	{indep_columns, Col} ->
	    Col	    
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_dep_column() ->
    global:send(data_server, {get_dep_column, self()}),
    receive
	{dep_column, Col} ->
	    Col	    
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_idle_children() ->
    global:send(data_server, {get_idle_children, self()}),
    receive
	{get_idle_children, Pid}->
	    Pid
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

get_child(Node) ->
    global:send(data_server, {get_child, self(), Node}),
    receive
       {get_child, Pid}->
	    Pid
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

set_child_data(Data) ->
    global:send(data_server, {set_child_data, self(), Data}),
    receive
	{set_child_data, done}->
	    ok
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

gather_data(Child, Data, Columns) ->
    Child ! {gather_stats, Columns, Data, self()},
    receive
	{stats, MS} ->
	    MS
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

distributed_create_tree() ->
    global:send(data_server, {distributed_create_tree, self()}),
    receive
	{distributed_create_tree, Tree}->
	    Tree
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.
    

local_create_tree() ->
    Pid = data_server:get_child(node()),
    Pid ! {create_tree, self()},
    receive
	{tree, Tree} ->
	    Tree
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

new() ->
    Self = self(),
    spawn(fun() -> 
		       yes = global:register_name(data_server, self()),
		       Self ! done,
		       data_server() 
	  end),
    receive
	done ->
	    ok
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.

%%%============================================
%%% helper functions
%%%============================================


dict_debug(Msg, Dict) ->
    Res = dict:fold(fun(K, V, L) ->
			    [ io_lib:format("~p -> ~p~n", [K, V]) | L ]
		    end, [], Dict),
    log(Msg ++ ": " ++ lists:flatten(Res)).
    
max(X, Y) when X >= Y ->
    X;
max(_X, Y) ->
    Y.

to_setlist(List) ->
    lists:map(fun(X) -> {X, []} end, List).
to_countlist(List) ->
    lists:map(fun(X) -> {X, 0} end, List).
    

distribute(Holes, Entities) ->
    HC = dict:from_list(to_setlist(Holes)),
    EC = dict:from_list(to_countlist(Entities)),
    distribute2(Holes, Entities, HC, EC, length(Holes), length(Entities), Holes, Entities).

distribute2(_, _, HC, EC, 0, 0, _, _) ->
    {HC, EC};
distribute2([H], [E|Es], HC, EC, Hc, Ec, Holes, Entities) ->
    HC2 = dict:append(H, E, HC),
    EC2 = dict:update_counter(E, 1, EC),
    distribute2(Holes, Es, HC2, EC2, max(Hc-1, 0), max(Ec-1, 0), Holes, Entities);
distribute2([H|Hs], [E], HC, EC, Hc, Ec, Holes, Entities) ->
    HC2 = dict:append(H, E, HC),
    EC2 = dict:update_counter(E, 1, EC),
    distribute2(Hs, Entities, HC2, EC2, max(Hc-1, 0), max(Ec-1, 0), Holes, Entities);
distribute2([H|Hs], [E|Es], HC, EC, Hc, Ec, Holes, Entities) ->
    HC2 = dict:append(H, E, HC),
    EC2 = dict:update_counter(E, 1, EC),
    distribute2(Hs, Es, HC2, EC2, max(Hc-1, 0), max(Ec-1, 0), Holes, Entities).



%% treenode_state = {Id, stats_needed, Workers :: [pid()], WorkersToGo :: [pid()], ColumnsLeft, GatheredStats :: ets()} |
%%                  {Id, node, TreeNodeData :: treenode_data}
%%                  {Id, leaf, TreeNodeData :: treenode_data}
%% treenode_data = {Column :: int(), TestType :: less | equals, Pivot | [Pivot], Childs :: [Ids], Count}
%% treeleaf_data = {decision, Class :: term(), Good :: int(), All :: int()}



%%%============================================
%%% data_server implementation
%%%============================================


-record(state, 
	  {types, 
	   names,
	   deps,
	   columns,
	   children,
	   idle_children}).

-record(child_state, {data, ids = []}).

data_server() ->
    %%io:fwrite("Receiving names~n"),
    receive
	{init_data, Names, Types, Deps, Nodes} ->
	    X = Names,
	    Y = Types,
	    Z = Deps,
	    C = Nodes,

	    1 = lists:foldl(fun(D, Acc) ->
				    case D of
					dep ->
					    Acc + 1;
					_ ->
					    Acc
				    end
			    end, 0, Deps),
	    
	    Children2 = lists:map(fun(Node) ->
					{Node, spawn_link(Node, ?MODULE, data_worker, [#child_state{}])}
				end, C),
	    
	    data_server3(#state{names = X, types = Y, deps = Z, columns = length(Z), children = Children2, idle_children = Children2});
	{exit, Pid} ->
	    Pid ! exitted,
	    global:unregister_name(data_server)
    end.


data_server3(
  #state{types = Types, 
	 names = Names, 
	 deps = Deps, 
	 columns = Columns, 
	 children = Children,
	 idle_children = Idle} = State) ->
%%    log("Waiting for messages"),
    receive
	{distributed_create_tree, Pid} ->
	    start_tree_constructor(Pid),
	    data_server3(State);	    
	{claim_children, Pid, CChildren} ->
	    case lists:all(fun(C) ->
				   lists:member(C, Idle)
			   end, CChildren) of
		false ->
		    Pid ! {reject, Idle},
		    data_server3(State);		
		true ->
		    State2 = State#state{idle_children = lists:subtract(Idle, CChildren)},
		    Pid ! {accept, CChildren},
		    data_server3(State2)
	    end;		    
	{get_idle_children, Pid} ->
	    Pid ! {get_idle_children, Idle},
	    data_server3(State);
	{get_child, Pid, Node} ->
	    CPid = proplists:get_value(Node, Children),
	    Pid ! {get_child, CPid},
	    data_server3(State);
	{set_child_data, CPid, Data} ->
	    lists:foreach(fun({_, Pid}) ->
				  Pid ! {data, Data}
			  end, Children),
	    CPid ! {set_child_data, done},
	    data_server3(State);
	{get_name, Pid, Col} ->
	    Pid ! {name, Col, lists:nth(Col, Names)},
	    data_server3(State);
	{get_names, Pid} ->
	    Pid ! {names, Names},
	    data_server3(State);
	{get_type, Pid, Col} ->
	    Pid ! {type, Col, lists:nth(Col, Types)},
	    data_server3(State);	    
	{get_types, Pid} ->
	    Pid ! {types, Types},
	    data_server3(State);
	{get_indep_columns, Pid} ->
	    Z = lists:zip(Deps, lists:seq(1, Columns)),
	    ZDeps = lists:filter(fun({D, _}) ->
					 D == ind
				 end, Z),
	    {_, Indeps} = lists:unzip(ZDeps),
	    Pid ! {indep_columns, Indeps},
	    data_server3(State);
	{get_dep_column, Pid} ->
	    Z = lists:zip(Deps, lists:seq(1, Columns)),
	    ZDeps = lists:filter(fun({D, _}) ->
					 D == dep
				 end, Z),
	    {_, [Dep]} = lists:unzip(ZDeps),
	    Pid ! {dep_column, Dep},
	    data_server3(State);
	{get_columns, Pid} ->
	    Pid ! {columns, Columns},
	    data_server3(State);
	{exit, Pid} ->
	    lists:foreach(fun({_Node, CPid}) ->
				  CPid ! exit
			  end, Children),
	    Pid ! exitted,
	    global:unregister_name(data_server);
	X ->
	    log("Unknown message: ~p", [X]),
	    data_server3(State)
    end.


%%%============================================
%%% data_worker
%%%============================================


data_worker(#child_state{data = undefined} = State) ->
    log("data_worker"),
    receive
	{data, Data} ->
	    log("data_worker: data"),
	    Tid = ets:new(data, [public]),
	    ets:insert(Tid, Data),
	    data_worker2(State#child_state{data = Tid});
	exit ->
	    ok
    end.


perform_redistribution(DS, TreeNode, {Column, TestType, Pivot} = _Test, NodeWorkers) ->
    log("perform_redistribution"),
    log("create DictNodes"),
    DictNodes = lists:flatten(
		  lists:foldl(
		    fun({TN, Ns}, Acc) ->
			    {N2, _} = lists:foldl(fun(_, {List, C}) -> 
							  {[{TN, C} | List], C + 1} 
						  end, {[], 1}, Ns),
			    [N2 | Acc]
		    end, [], NodeWorkers
		   )
		 ),

    TNs = lists:map(fun({TN, _}) -> TN end, NodeWorkers),
    TNSizes = lists:map(fun({TN, Nds}) -> {TN, length(Nds)} end, NodeWorkers),

    NodesTN = dict:from_list(NodeWorkers),
    %%log("Nodes", []),
    %%dict_debug(NodesTN),

    NodeLists = dict:from_list(to_setlist(DictNodes)),
    %%dict_debug("Dict nodes", NodeLists),

    NodeCounts = dict:from_list(to_countlist(TNs)),
    %%dict_debug("Node count", NodeCounts),

    TnWorkerSizes = dict:from_list(TNSizes),
    %%log("Node sizes"),
    %%dict_debug(TnWorkerSizes),

    {_, IdsListsToSend} = lists:foldl(fun(Id, {NCounts, NList}) ->
					      [Row] = ets:lookup(DS, Id),
					      TN = dataset_ets:row_test(Row, TestType, Column, TNs, Pivot),
					      NCounts2 = dict:update_counter(TN, 1, NCounts),
					      Num = dict:fetch(TN, NCounts2),
					      WN = (Num rem dict:fetch(TN, TnWorkerSizes)) + 1,
					      NList2 = dict:append({TN, WN}, Id, NList),
					      {NCounts2, NList2}
				      end, {NodeCounts, NodeLists}, get_ids(nil, TreeNode)),
    %%log("Redist lists"),
    %%dict_debug(IdsListsToSend),

    dict:fold(fun({N, W}, List, _) ->
		      P = lists:nth(W, dict:fetch(N, NodesTN)),
		      M = {add_ids, N, List},
		      %%log("~p ! ~w", [P, M]),
		      P ! M
	      end, 0, IdsListsToSend), 

    ok.
    

data_worker2(#child_state{data = DS} = State) ->
    log("data_worker2"),
    receive
	{select_ids, TreeNode, K0, N} = M ->
	    log("data_worker2: select_ids ~p", [M]),
	    K = K0 - 1,
	    Ids = ets:foldl(fun(Row, L) ->
				     Id = element(1, Row),
				     if 
					 (N =:= K andalso K =:= 1) orelse Id rem N =:= K ->
					     [Id | L];
					 true ->
					     L
				     end
			     end, [], DS),
	    log("selected ids ~p", [length(Ids)]),
	    State2 = store_ids(State, TreeNode, Ids),
	    data_worker2(State2);
	{forget_ids, TreeNode} ->
	    log("forget_ids"),
	    data_worker2(remove_ids(State, TreeNode));
 	{redistribute_ids, TreeNode, Test, Nodes} ->
	    log("redistribute_ids"),
	    perform_redistribution(DS, TreeNode, Test, Nodes),
	    data_worker2(State);
	{set_ids, TreeNode, NewIds} ->
	    log("set_ids"),
	    State2 = store_ids(State, TreeNode, NewIds),
	    data_worker2(State2);
	{gather_node_stats, TreeNode, Columns, Pid} = M ->
	    log("gather_node_stats: ~p", [M]),
	    State2 = receive
		{gather_ids, TreeNode, C} ->
		    log("gather_ids"),
		    gather_ids(TreeNode, State, C)
	    after ?TIMEOUT ->
		    log("Gather_ids timeout"),
		    erlang:error(timeout)
	    end,
	    Ids = get_ids(State, TreeNode),
	    MS = ets:new(mstat, [private]),
	    %%log("Ids for node ~p: ~p", [TreeNode, Ids]),
	    dataset_ets:gather_mstatistics(Ids, DS, Columns, data_server:get_dep_column(), MS),
	    Pid ! {stats, self(), TreeNode, ets:tab2list(MS)},
	    ets:delete(MS),
	    data_worker2(State2);	    

%%% ================================
%%%  nonparallel functions
%%% ================================
	{gather_stats, Columns, Data, Pid} ->
	    MS = ets:new(mstat, [private]),
	    dataset_ets:gather_mstatistics(Data, DS, Columns, data_server:get_dep_column(), MS),
	    Pid ! {stats, ets:tab2list(MS)},
	    ets:delete(MS),
	    data_worker2(State);
	{create_tree, Pid} ->
	    Node = dec_tree:expand_node(lists:seq(1, ets:info(DS, size)), DS),
	    Pid ! {tree, Node},
	    data_worker2(State);
	{expand_node, Columns, Data, Pid} ->
	    Node = dec_tree:expand_node(Data, DS, Columns),
	    Pid ! {node, Node},
	    data_worker2(State);
		%%dec_tree:process_node(Columns, Data),
%%% ================================
%%% ================================

	exit ->
	    ok
    end.
gather_ids(_TreeNode, State, 0) ->
    State;
gather_ids(TreeNode, State, Count) ->
    store_ids(State, TreeNode, []),
    gather_ids2(TreeNode, State, Count).
gather_ids2(_, State, 0) ->
    State;
gather_ids2(TreeNode, State, Count) ->
    State2 = receive
		 {add_ids, TreeNode, NewIds} ->
		     %%log("add_ids ~p", [NewIds]),
		     store_ids(State, TreeNode, NewIds ++ get_ids(State, TreeNode))
	     after ?TIMEOUT ->
		     erlang:error(timeout)
	     end,
    gather_ids2(TreeNode, State2, Count - 1).


store_ids(State, TreeNode, Ids) ->
    %%log("store_ids ~p ~p", [TreeNode, length(Ids)]),
    put({treenode, TreeNode}, Ids),
    State.

get_ids(_State, TreeNode) ->
    X = get({treenode, TreeNode}),
    true = is_list(X),
    %%log("get_ids ~p ~p", [TreeNode, length(X)]),
    X.

remove_ids(State, TreeNode) ->
    erase({treenode, TreeNode}),
    State.

%%%============================================
%%% tree_constructor
%%%============================================
	

-record(tc_state,
	{rpid,
	 tree,
	 workers = [],
	 next_id = 0,
	 nodes_to_expand = []}).


start_tree_constructor(ResponsePid) ->
    spawn(fun() ->
		  tree_constructor(#tc_state{rpid = ResponsePid, tree = ets:new(tree, [private])})
	  end).

tree_constructor(#tc_state{tree = Tree} = State) ->
    WorkerNodes = get_idle_children(),
    {accept, WorkerNodes} = claim_children(WorkerNodes),
    {_Nodes, Workers} = lists:unzip(WorkerNodes),
%%    lists:foreach(fun(W) ->
%%			  %%true = erlang:is_process_alive(W),
%%			  erlang:monitor(process, W)
%%		  end, Workers),
    log("Workers: ~p", [Workers]),
    N = length(Workers),
    lists:foldl(fun(Worker, Acc) ->
		       Worker ! {select_ids, 0, Acc, N},
		       Worker ! {gather_ids, 0, 0},
		       Acc + 1
	       end, 1, Workers),
    RootNode = construct_new_node(data_server:get_indep_columns(), 0),
    RootNode2 = set_workers_to_node(RootNode, Workers),
    start_work_on_node(RootNode2),
    ets:insert(Tree, RootNode2),
    tree_constructor2(State#tc_state{workers = Workers, 
				     nodes_to_expand = [node_id(RootNode2)],
				     next_id = 1}).

tree_constructor2(#tc_state{tree = Tree, nodes_to_expand = [], rpid = ResponsePid} = _State) ->
    T = reconstruct_tree(Tree),
    log("~p", [T]),
    ResponsePid ! {distributed_create_tree, T},
    ok;

tree_constructor2(#tc_state{tree = Tree, workers = _Workers, next_id = NextId, nodes_to_expand = NTX} = State) ->
    log("tree_constructor2"),
    %%log("Done: ~p", [ets:tab2list(Tree)]),
    %%log("Left: ~p", [NTX]),
    receive
	{'DOWN', _MonitorRef, _Type, _Object, _Info} ->
	    erlang:error({node_failed, _Object});
	{stats, Worker, TNode, Stats} = _S ->
	    log("stats ~p from ~p", [TNode, Worker]),
	    [TreeNode] = ets:lookup(Tree, TNode),
	    log("before receive_stats"),
	    X = receive_stats(Tree, TreeNode, Stats, Worker, NextId),
	    log("after receive_stats"),
	    {NewNodes, DoneNodes, UpdateNodes, NextId2} = X,
	    log("after matching"),
	    lists:foreach(fun(N) -> ets:insert(Tree, N) end, DoneNodes),
	    lists:foreach(fun(N) -> ets:insert(Tree, N) end, NewNodes),				  
	    lists:foreach(fun(N) -> ets:insert(Tree, N) end, UpdateNodes),
	    NTX2 = lists:map(fun node_id/1, NewNodes) ++ lists:subtract(NTX, lists:map(fun node_id/1, DoneNodes)),
	    tree_constructor2(State#tc_state{nodes_to_expand = NTX2, next_id = NextId2});
	exit ->
	    ets:delete(Tree);
	M ->
	    log("Unknown message ~p", [M])
    after ?TIMEOUT ->
	    erlang:error(timeout)
    end.


receive_stats(_Tree, TNode, Stats, Worker, NId) ->
    {Id, stats_needed, Workers, LeftWorkers, ColumnsLeft, GS} = TNode,
    true = lists:member(Worker, LeftWorkers),
    LeftWorkers2 = lists:delete(Worker, LeftWorkers),
    dataset_ets:sum_statistics(GS, Stats),
    TNode2 = {Id, stats_needed, Workers, LeftWorkers2, ColumnsLeft, GS},
    Res = 
	case LeftWorkers2 of
	    [] ->
		case is_leaf(TNode2) of
		    true ->
			{[], [create_leaf(TNode2)], [], NId};
		    _ ->
			IGForColumns = lists:flatten(lists:map(fun(Col) -> 
								       dec_tree:calculate_entropy(Col, GS) 
							       end, ColumnsLeft)),
			{_IG, Column, TestType, Pivot} = _Best = dataset_ets:get_min_row(IGForColumns, 1),
			Test = {Column, TestType, Pivot},
			Count = dataset_ets:mstats_count(GS),
			ColumnsLeft2 = lists:delete(Column, ColumnsLeft),
			{Nodes, NId2} = create_nodes(Test, ColumnsLeft2, NId),
			NodeIds = lists:map(fun(X) -> element(1, X) end, Nodes),
			TNode3 = {Id, node, {Column, TestType, Pivot, NodeIds, Count}},
			
			Nodes2 = redistribute_workers(Workers, Id, Workers, Nodes, Test),
			lists:foreach(fun(N) -> start_work_on_node(N) end, Nodes2),
			lists:foreach(fun(W) -> W ! {forget_ids, Id},
						{Nodes2, [TNode2], NId2}
				      end, Workers),
			{Nodes2, [TNode3], [], NId2}	    
		end;				  
	    _ ->
		{[], [], [TNode2], NId}
	end,
    Res.


redistribute_workers(ParentWorkers, ParentTreeNode, Workers, Nodes, Test) ->
    PWLength = length(ParentWorkers),
    {NodesWorkers, _} = distribute(Nodes, Workers),
    Nodes2 = lists:map(fun(N) ->
			       set_workers_to_node(N, dict:fetch(N, NodesWorkers))
		       end, Nodes),
    MsgNodes = lists:keysort(1, lists:map(fun({Id, _, W, _, _, _}) ->
				 {Id, W}
			 end, Nodes2)),
    Msg = {redistribute_ids, ParentTreeNode, Test, MsgNodes},
    log("Redist msg: ~p", [Msg]),

    log("MsgNodes: ~p", [MsgNodes]),

    %% assertion
    NWs = dict:size(NodesWorkers),
    case element(2, Test) of
	equals ->
	    NWs = length(element(3, Test));
	less ->
	    NWs = 2
    end,
    %% done
    
    lists:foreach(fun(PW) -> PW ! Msg end, ParentWorkers),
    WorkersWait = lists:flatten(
		    lists:map(
		      fun({TN, Ws}) ->
			      lists:foldl(fun(W, L) ->
						  [{TN, W, PWLength} | L]
					  end, [], Ws)
		      end, MsgNodes
		     )
		   ),
    log("Gather ids messages: ~p", [WorkersWait]),
    lists:foreach(fun({TN, W, C}) -> 
			  M = {gather_ids, TN, C},
			  log("~p ! ~w", [W, M]),
			  W ! M
		  end, WorkersWait),
    log("WorkersWait: ~p", [WorkersWait]),
    
    Nodes2.

create_nodes({_Column, equals, Pivot}, CL, NId) ->
    NC = length(Pivot),
    Nodes = lists:seq(NId, NId + NC - 1),
    Nodes2 = lists:map(fun(Id) ->
			       construct_new_node(CL, Id)
		       end, Nodes),
    {Nodes2, NId + length(Pivot) + 1};
create_nodes({_Column, less, _Pivot}, CL, NId) ->
    Nodes = [NId, NId + 1],
    Nodes2 = lists:map(fun(Id) ->
			       construct_new_node(CL, Id)
		       end, Nodes),
    {Nodes2, NId + 2}.


create_leaf({Id, stats_needed, _Workers, _LeftWorkers, _ColumnsLeft, GS} = _TNode) ->
    CS = dataset_ets:mstats_classes_count(GS),
    Sorted = lists:reverse(lists:keysort(2, CS)),
    {Dec, F} = hd(Sorted),
    {Id, leaf, {decision, Dec, F, dataset_ets:mstats_count(GS)}}.

is_leaf({_Id, stats_needed, _Workers, [] = _LeftWorkers, [] = _ColumnsLeft, _GS} = _TNode) ->
    true;
is_leaf({_Id, stats_needed, _Workers, [] = _LeftWorkers, _ColumnsLeft, GS} = _TNode) ->
    %%log("No more workers left for ~p, check if it is node", [_Id]),
    CS = dataset_ets:mstats_classes_count(GS),
    case length(CS) of
	0 -> erlang:error(weired_error);
	1 -> true;
	_ -> false
	    %%Sorted = lists:keysort(2, CS),
	    %%log("Stats: ~p", [Sorted]),
	    %%erlang:error(not_implemented)
    end;
is_leaf(_) ->
    false.


construct_new_node(ColumnsLeft, Id) ->
    {Id, stats_needed, [], [], ColumnsLeft, ets:new(gstats, [private])}.

set_workers_to_node({Id, stats_needed, _, _, ColumnsLeft, GS} = _Node, Workers) ->
    {Id, stats_needed, Workers, Workers, ColumnsLeft, GS}.

start_work_on_node({Id, stats_needed, Workers, _, ColumnsLeft, _}) ->
    lists:foreach(fun(Worker) ->
			  Worker ! {gather_node_stats, Id, ColumnsLeft, self()}
		  end, Workers).

    
node_id({Id, stats_needed, _, _, _ColumnsLeft, _GS} = _Node) ->
    Id;
node_id({Id, leaf, _} = _Node) ->
    Id;
node_id({Id, node, _} = _Node) ->
    Id.

rec_node(_Ets, {_Id, leaf, N}) ->
    N;
rec_node(Ets, {_Id, node, {Column, TestType, Pivot, Childs, Count}}) ->
    C2 = lists:map(fun(Id) ->
		      reconstruct_tree(Ets, Id)
	      end, Childs),
    {Column, TestType, Pivot, C2, Count}.

reconstruct_tree(Ets) ->
    reconstruct_tree(Ets, 0).
reconstruct_tree(Ets, Id) ->
    [N] = ets:lookup(Ets, Id),
    rec_node(Ets, N).    

