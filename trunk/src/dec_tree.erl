%% Author: gleber
%% Created: May 17, 2007
%% Description: TODO: Add desciption to hello_world
-module(dec_tree).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([test_simple/0, 
	 test_dist/0,
	 classify_dataset_ets/2,
%	 time_test/0, 
	 expand_node/2,
	 expand_node/3,
	 prune_tree/1,
	 write_tree/1,
	 csv_filter/1,
	 calculate_entropy/2,
	 readfile/1,
	 csv_filter/1
	]).
%-export([mexpand_node/1]).


-import(log, [log/1, log/2]).

%%
%% API Functions
%%

%%
%% Local Functions
%%


%%%============================================
%%% file handling
%%%============================================

readfile(FileName) ->
    {ok, Binary} = file:read_file(FileName),
    erlang:binary_to_list(Binary).

csv_filter(Csv) ->
    csv_filter(Csv, []).

csv_filter([], Acc) ->
    lists:reverse(Acc);

csv_filter([[]|Csv], Acc) ->
    csv_filter(Csv, Acc);

csv_filter([H|Csv], Acc) ->
    csv_filter(Csv, [H|Acc]).


%%%============================================
%%% dataset manipulations
%%%============================================


get_decision_column(Data, DS) ->
    dataset_ets:get_column(Data, DS, data_server:get_dep_column()).

%%%============================================
%%% calculations
%%%============================================


entropy_test(Stats, Classes, Values, equals, Column) ->
    N = dataset_ets:mstats_count(Stats),
    F = fun(Value) ->
		V = dataset_ets:mstats_value(Stats, Column, Value),
		case V of
		    0 -> 0;
		    _ ->
			K = fun(Class) ->
				    I = dataset_ets:mstats_inter(Stats, Column, Value, Class),
				    case I of 
					0 -> 0;
					_ -> 
					    D = I / V,
					    D * (- math:log(D))
				    end
			    end,
			Etr = lists:sum(lists:map(K, Classes)),		
			(V / N) * Etr
		end
	end,
    lists:sum(lists:map(F, Values));

entropy_test(Stats, Classes, Value, less, Column) ->
    N = dataset_ets:mstats_count(Stats),
    K = fun(Op, V, Class) ->
		case V of 
		    0 -> 0;
		    _ ->
			I = dataset_ets:mstats_inter(Stats, Column, Value, Class, Op),
			case I of 
			    0 -> 0;
			    _ -> 
				D = I / V,
				D * (- math:log(D))
			end
		end
	end,

    F = fun(Op) ->
		Vn = dataset_ets:stats_value(Stats, Value, Op),
		Kn = fun(Class) -> K(Op, Vn, Class) end,
		Etr = lists:sum(lists:map(Kn, Classes)),
		(Vn / N) * Etr
	end,
    Values = [less, notless],
    lists:sum(lists:map(F, Values)).


calculate_entropy(Column, Stats) ->
    %%    io:fwrite("Selecting test for ~p~n", [Column]),
    Type = data_server:get_type(Column),
    List = calculate_entropy2(Column, Type, Stats),
    List.

calculate_entropy2(Column, int, Stats) ->
    Values = dataset_ets:mstats_values(Stats, Column),
    Classes = dataset_ets:mstats_classes(Stats),
    lists:map(fun(Value) ->
		      {entropy_test(Stats, Classes, Value, less, Column), Column, less, Value}
	      end, Values);

calculate_entropy2(Column, Type, Stats) when Type =:= str; Type =:= bool ->
    Values = dataset_ets:mstats_values(Stats, Column),
    Classes = dataset_ets:mstats_classes(Stats),
    IG = entropy_test(Stats, Classes, Values, equals, Column),
    {IG, Column, equals, Values}.


%%%============================================
%%% tree analysis
%%%============================================


count_items(List) ->
    Unique = lists:usort(List),
    CList = lists:map(fun(X) ->
			      {X, 1}
		      end, List),
    Counts = lists:map(fun(X) ->
		      {X, length(proplists:get_all_values(X, CList))}
	      end, Unique),
    lists:reverse(lists:keysort(2, Counts)).


select_column_and_test(Data, DS, FreeColumns) ->

    Stats = ets:new(dt_statistics, [private]),
    DecColumn = data_server:get_dep_column(),
    ok = dataset_ets:gather_mstatistics(Data, DS, FreeColumns, DecColumn, Stats),
    IGForColumns = lists:flatten(lists:map(fun(Col) -> calculate_entropy(Col, Stats) end, FreeColumns)),
    Count = dataset_ets:mstats_count(Stats),
    ets:delete(Stats),

    io:fwrite("~p left: ~n~p~n", [length(FreeColumns), IGForColumns]),
    {_IG, Column, TestType, Pivot} = _Best = dataset_ets:get_min_row(IGForColumns, 1),
%%    io:fwrite("!!! Best test: ~p~n", [Best]),
    {Column, TestType, Pivot, Count}.


%%%============================================
%%% tree construction
%%%============================================
 
expand_node(Data, DS) ->
    %%    Columns = lists:seq(2, data_server:get_columns() - 1),
    Columns = data_server:get_indep_columns(),
    expand_node(Data, DS, Columns).
expand_node([], _DS, []) ->
    {undecided, []};
expand_node(Data, DS, []) ->
    DecCol = get_decision_column(Data, DS),
    DecValues = lists:usort(DecCol),
    case length(DecValues) of
	1 -> {decision, hd(DecValues), length(Data), length(Data)};
	_Count -> 
	    [{D, Fre} | _] = count_items(DecCol),
	    {decision, D, Fre, length(Data)}
    end;
expand_node([], _DS, _LeftColumns) ->
    {undecided, []};
expand_node(Data, DS, LeftColumns) ->
    log("expand_node: ~p:~p", [node(), self()]),
    log("        data: ~p", [length(Data)]),
    DecCol = get_decision_column(Data, DS),
    DecValues = lists:usort(DecCol),
    case length(DecValues) of
	1 -> {decision, hd(DecValues), length(Data), length(Data)};
	_Count ->
	    %%	    io:fwrite("Free columns: ~p, decision left ~p~n", [LeftColumns, Count]),
	    {Column, TestType, Pivot, Count} = select_column_and_test(Data, DS, LeftColumns),
	    Sets = dataset_ets:filter_data(Data, DS, Column, TestType, Pivot),
	    NewCols = lists:delete(Column, LeftColumns),
%            Nodes = plists:map(fun(X) -> expand_node(X, NewCols) end, Sets, get_bmalt()),
	    Nodes = lists:map(fun(X) -> expand_node(X, DS, NewCols) end, Sets),
	    {Column, TestType, Pivot, Nodes, Count}
    end.    

%%%============================================
%%% prune_tree
%%% arg: Tree
%%% returns: Tree - pruned
%%%============================================


prune_tree(Tree) ->
    {_, TreeNode} = prune_tree2(Tree),
    TreeNode.

prune_tree2({decision, _Dec, _Fre, _Number} = TreeNode) ->
    {decision, TreeNode};

prune_tree2({undecided, _Data} = _TreeNode) ->
    erlang:error(bad_tree);

prune_tree2({Column, TestType, Pivots, Childs, Count} = _TreeNode) 
  when is_list(Childs) ->

    Ch = lists:map(fun prune_tree2/1, Childs),
    TheSame = the_same_decision(Ch),
    if
	TheSame ->
	    {decision, sum_decisions(get_pruned_nodes(Ch))};
	true ->
	    {dont_prune, {Column, TestType, Pivots, get_pruned_nodes(Ch), Count}}
    end.

sum_decisions(List) ->
    _H = {decision, D, _, _} = hd(List),
    {Fn, Nn} = lists:foldl(fun({decision, Decision, F, N}, {Good, Count})
			      when D =:= Decision ->
			{Good + F, Count + N}
		end, {0, 0}, List),
    {decision, D, Fn, Nn}.
			

get_pruned_nodes(Childs) ->
    lists:map(fun(C) -> element(2, C) end, Childs).    

the_same_decision(List) when element(1, hd(List)) =:= decision ->
    {decision, {decision, D, _, _}} = hd(List),
    lists:all(fun(El) -> 
		      element(1, El) =:= decision andalso element(2, element(2, El)) =:= D
	      end, List);

the_same_decision(_) ->
    false.

%%%============================================
%%% classify
%%% arg: Item - data item
%%%      Tree - dec_tree
%%% returns: Class
%%%============================================

classify(Item, Tree) ->
    classify2(Item, Tree).

classify_get(_V, [], []) ->    
    erlang:error(bad_tree);

classify_get(_V, [_V | _Pivots], [C | _Childs]) ->
    C;

classify_get(V, [_P | Pivots], [_C | Childs]) ->    
    classify_get(V, Pivots, Childs).

classify2(_Item, {decision, Dec, _Fre, _Number}) ->
    Dec;

classify2(Item, {Column, equals, Pivots, Childs, _Count}) 
  when is_list(Pivots) andalso 
       is_list(Childs) andalso 
       length(Pivots) == length(Childs) ->
    V = element(Column, Item),
    C = classify_get(V, Pivots, Childs),
    classify2(Item, C);

classify2(Item, {Column, less, Pivot, [Node1, Node2], _Count} = _TreeNode) ->
    V = element(Column, Item),
    if 
	V < Pivot ->
	    classify2(Item, Node1);
	true ->
	    classify2(Item, Node2)
    end.
	    

classify_dataset_ets(Dataset_Ets, Tree) ->       
    lists:foldl(fun(Item, {Sum, Good}) ->
			Class = classify(Item, Tree),
			TrueClass = element(data_server:get_dep_column(), Item),
			if 
			    Class =:= TrueClass ->
				{Sum +1, Good + 1};
			    true ->
				{Sum +1, Good}
			end
		end, {0, 0}, Dataset_Ets).


%%%============================================
%%% tests
%%%============================================

do_test_file(FN) ->
    data_server:new(),
    File = csv_filter(csv:read(readfile(FN))),
    {ok, {Csv, _Size}, Names, Types, Deps} = dataset_ets:csv_process(File),
    data_server:init(Names, Types, Deps),
    data_server:set_child_data(Csv),
    {Time, T} = timer:tc(data_server, local_create_tree, []),
    io:fwrite("Original tree:~n"),
    write_tree(T),
    T2 = prune_tree(T),
    io:fwrite("Pruned tree:~n"),
    write_tree(T2),
    data_server:stop(),
    Time.

test_simple() ->
    do_test_file("/home/gleber/workspace/erlang/dec_tree/priv/mices.csv").

do_test_dist_file(FN) ->
    data_server:new(),
    File = csv_filter(csv:read(readfile(FN))),
    {ok, {Csv, _Size}, Names, Types, Deps} = dataset_ets:csv_process(File),
    data_server:init(Names, Types, Deps),
    data_server:set_child_data(Csv),
    {Time, T} = timer:tc(data_server, distributed_create_tree, []),
    io:fwrite("Original tree:~n"),
    write_tree(T),
    T2 = prune_tree(T),
    io:fwrite("Pruned tree:~n"),
    write_tree(T2),
    data_server:stop(),
    Time.

test_dist() ->
    do_test_dist_file("/home/gleber/workspace/erlang/dec_tree/priv/mices.csv").



write_tree(Tree) ->
    write_tree(Tree, "   ").

write_tree({decision, Dec, Fre, Number}, Indent) ->
    io:fwrite("~s decision: ~p in ~p of ~p entries~n", [Indent, Dec, Fre, Number]);

write_tree({undecided, Data}, Indent) ->
    io:fwrite("~s undecided ~p~n", [Indent, Data]);

write_tree({Column, TestType, Pivots, Childs, Count}, Indent) when is_list(Pivots) andalso is_list(Childs) andalso length(Pivots) == length(Childs) ->
    ColName = data_server:get_name(Column),
    io:fwrite("~stest ~p on ~p (~p)~n", [Indent, TestType, ColName, Count]),
    lists:foreach(fun({Pivot, Child}) ->
			  io:fwrite("~s  ~p:~n", [Indent, Pivot]),
			  write_tree(Child, "   " ++ Indent)
		  end, lists:zip(Pivots, Childs));

write_tree({Column, TestType, Pivot, [Node1, Node2], Count} = _TreeNode, Indent) ->
    ColName = data_server:get_name(Column),
    io:fwrite("~stest ~p on ~p (~p)~n", [Indent, TestType, ColName, Count]),
    io:fwrite("~s  ~p:~n", [Indent, Pivot]),
    write_tree(Node1, "   " ++ Indent),
    io:fwrite("~s  other:~n", [Indent]),
    write_tree(Node2, "   " ++ Indent).
    

