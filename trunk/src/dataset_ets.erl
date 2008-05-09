%% Author: gleber
%% Created: May 17, 2007
%% Description: TODO: Add desciption to hello_world
-module(dataset_ets).

%%
%% Include files
%%

-include_lib("stdlib/include/ms_transform.hrl").

%%
%% Exported Functions
%%
-export([get_min_row/3, get_max_row/3,
	 get_min_row/2, get_max_row/2,
	 filter_data/5,

	 stats_values/1,
	 stats_classes/1,
	 stats_value/3, stats_value/2,
	 stats_inter/4, stats_inter/3,
	 stats_count/1,
	 stats_classes_count/1,

	 mstats_values/2,
	 mstats_classes/1,
	 mstats_value/4, mstats_value/3,
	 mstats_inter/5, mstats_inter/4,
	 mstats_count/1,
	 mstats_classes_count/1,

	 row_test/5,
 
	 csv_process/1,
	 get_column/3,
	 gather_statistics/5,
	 gather_mstatistics/5,
	 sum_statistics/2
	]).
 
%%
%% API Functions
%%

get_min_row(Data, Column) ->
    F = fun(Row, Acc) ->	
		if
		    Acc == false ->
			Row;
		    element(Column, Row) < element(Column, Acc) ->
			Row;
		    true ->
			Acc
		end
	end,
    lists:foldl(F, false, Data).

get_max_row(Data, Column) ->
    F = fun(Row, Acc) ->
		if
		    Acc == false ->
			Row;
		    element(Column, Row) > element(Column, Acc) ->
			Row;
		    true ->
			Acc
		end
	end,
    lists:foldl(F, false, Data).


get_min_row(Data, DataStore, Column) ->
    F = fun(X, Acc) ->	
		[Row] = ets:lookup(DataStore, X),
		if
		    Acc == false ->
			Row;
		    element(Column, Row) < element(Column, Acc) ->
			Row;
		    true ->
			Acc
		end
	end,
    lists:foldl(F, false, Data).

get_max_row(Data, DataStore, Column) ->
    F = fun(X, Acc) ->
		[Row] = ets:lookup(DataStore, X),
		if
		    Acc == false ->
			Row;
		    element(Column, Row) > element(Column, Acc) ->
			Row;
		    true ->
			Acc
		end
	end,
    lists:foldl(F, false, Data).
    

get_column(Data, DataStore, Column) ->
    F = fun(X, Acc) ->
		[Row] = ets:lookup(DataStore, X),
        	[element(Column, Row) | Acc]
        end,
    lists:reverse(lists:foldl(F, [], Data)).

data_select(Data, DataStore, Column, Value) ->
    lists:filter(fun(X) -> 
			 [Row] = ets:lookup(DataStore, X),
			 element(Column, Row) == Value 
		 end, Data).			 

data_partition(Data, DataStore, Column, Operation) ->
    {X, Y} = lists:partition(fun(X) -> 
				     [Row] = ets:lookup(DataStore, X),
				     Operation(element(Column, Row)) 
			     end, Data),
    [X, Y].

filter_data(Data, DataStore, Column, equals, Values) ->
    lists:map(fun(X) ->
		      data_select(Data, DataStore, Column, X)
	      end, Values);

filter_data(Data, DataStore, Column, equal, Value) ->
    F = fun(X) ->
        	X == Value        	
        end,
    data_partition(Data, DataStore, Column, F);

filter_data(Data, DataStore, Column, less, Value) ->
    F = fun(X) ->
		X < Value
	end,
    data_partition(Data, DataStore, Column, F).


row_test(Row, less, Column, TNs, Pivot) ->
    if
	element(Column, Row) < Pivot ->
	    lists:nth(1, TNs);
	true ->
	    lists:nth(2, TNs)
    end;
row_test(Row, equals, Column, TNs, Pivots) ->
    lists:nth(lists_index(element(Column, Row), Pivots), TNs).


%%%============================================
%%% statistics
%%%============================================

sum_statistics(MainStats, Stats) ->
    lists:foreach(fun({Stat, N}) ->
			  increase_counter(MainStats, Stat, N)
		  end, Stats).	    

gather_statistics(Data, DataStore, Column, DepColumn, Stats) ->
    F = fun(RowId) ->
		[Row] = ets:lookup(DataStore, RowId),
		V = element(Column, Row),
		D = element(DepColumn, Row),
		X = {V, D},
		increase_counter(Stats, count),
		increase_counter(Stats, {value, V}),
		increase_counter(Stats, {class, D}),
		increase_counter(Stats, {inter, X})
	end,
    lists:foreach(F, Data).


gather_mstatistics(Data, DataStore, Columns, DepColumn, Stats) ->
    F = fun(RowId) ->
		[Row] = ets:lookup(DataStore, RowId),
		D = element(DepColumn, Row),
		increase_counter(Stats, count),
		increase_counter(Stats, {class, D}),

		lists:foreach(fun(Column) ->
				      V = element(Column, Row),	
				      X = {V, D},
				      increase_counter(Stats, {Column, value, V}),
				      increase_counter(Stats, {Column, inter, X})
			      end, Columns)
	end,
    lists:foreach(F, Data).

mstats_values(Stats, Column) ->
    ets:select(Stats, [{{{Column, value, '$1'}, '_'}, [], ['$1']}]).

stats_values(Stats) ->
    ets:select(Stats, [{{{value, '$1'}, '_'}, [], ['$1']}]).

mstats_classes(Stats) ->
    stats_classes(Stats).
stats_classes(Stats) ->
    ets:select(Stats, [{{{class, '$1'}, '_'}, [], ['$1']}]).

mstats_classes_count(Stats) ->
    stats_classes_count(Stats).    
stats_classes_count(Stats) ->
    M = ets:fun2ms(fun({{class, Cl}, Co}) -> {Cl, Co} end),
    ets:select(Stats, M).



mstats_value(Stats, Column, Value, less) ->
    lists:sum(ets:select(Stats, [{{{Column, value, '$1'}, '$2'}, [{'<', '$1', Value}], ['$2']}]));

mstats_value(Stats, Column, Value, notless) ->
    stats_count(Stats) - mstats_value(Stats, Column, Value, less).

mstats_value(Stats, Column, Value) ->
    [{{Column, value, Value}, V}] = ets:lookup(Stats, {Column, value, Value}), V.



stats_value(Stats, Value, less) ->
    lists:sum(ets:select(Stats, [{{{value, '$1'}, '$2'}, [{'<', '$1', Value}], ['$2']}]));

stats_value(Stats, Value, notless) ->
    stats_count(Stats) - stats_value(Stats, Value, less).

stats_value(Stats, Value) ->
    [{{value, _}, V}] = ets:lookup(Stats, {value, Value}), V.


mstats_class(Stats, Class) ->
    stats_class(Stats, Class).
stats_class(Stats, Class) ->
    [{{class, _}, V}] = ets:lookup(Stats, {class, Class}), V.




stats_inter(Stats, Value, Class) ->
    case ets:lookup(Stats, {inter, {Value, Class}}) of
	[] -> 0;
	[{{inter, _}, V}] -> V
    end.

stats_inter(Stats, Value, Class, less) ->
    lists:sum(ets:select(Stats, [{{{inter, {'$1', Class}}, '$2'}, [{'<', '$1', Value}], ['$2']}]));

stats_inter(Stats, Value, Class, notless) ->
    stats_class(Stats, Class) - stats_inter(Stats, Value, Class, less).




mstats_inter(Stats, Column, Value, Class) ->
    case ets:lookup(Stats, {Column, inter, {Value, Class}}) of
	[] -> 0;
	[{{Column, inter, _}, V}] -> V
    end.

mstats_inter(Stats, Column, Value, Class, less) ->
    lists:sum(ets:select(Stats, [{{{Column, inter, {'$1', Class}}, '$2'}, [{'<', '$1', Value}], ['$2']}]));

mstats_inter(Stats, Column, Value, Class, notless) ->
    mstats_class(Stats, Class) - mstats_inter(Stats, Column, Value, Class, less).




mstats_count(Stats) ->
    stats_count(Stats).
stats_count(Stats) ->
    [{count, V}] = ets:lookup(Stats, count), V.
    


%%%============================================
%%% csv processing
%%%============================================






csv_process(Csv) ->
    csv_process(Csv, 0).

csv_process([Head0|Csv], 0) ->
    Head = ["X" | Head0],
    csv_process(Csv, 1, Head).
csv_process([Types0|Csv], 1, Names) ->
    Types = ["id" | Types0],
    F = fun(X) ->
		lists:member(X, ["id", "int","bool","str"])
	end,
    true = lists:all(F, Types),
    Types2 = lists:map(fun(X) -> list_to_atom(X) end, Types),
    Columns = length(Names),
    Columns = length(Types2),
    csv_process(Csv, 2, Names, Types2).
csv_process([Deps0|Csv], 2, Names, Types) ->
    Deps = ["ign" | Deps0],
    F = fun(X) ->
		lists:member(X, ["ign","ind","dep"])
	end,
    true = lists:all(F, Deps),
    Deps2 = lists:map(fun(X) -> list_to_atom(X) end, Deps),
    Columns = length(Names),
    Columns = length(Types),
    Columns = length(Deps2),
    %%io:fwrite("~p~n~p~n~p", [Names, Types, Deps2]),
    csv_process2(Csv, Names, Types, Deps2).
    

csv_convert(Row, Types) ->
    lists:reverse(csv_convert(Row, Types, [])).

csv_convert([], [], Acc) ->
    Acc;
csv_convert([V|Row], [id|Types], Acc) ->
    csv_convert(Row, Types, [V | Acc]);
csv_convert([V|Row], [str|Types], Acc) ->
    csv_convert(Row, Types, [V | Acc]);
csv_convert([V|Row], [bool|Types], Acc) ->
    csv_convert(Row, Types, [str_to_bool(V) | Acc]);
csv_convert([V|Row], [int|Types], Acc) ->
    csv_convert(Row, Types, [list_to_integer(V) | Acc]).
    
    

csv_process2(Csv, Names, Types, Deps) ->
    Csv2 = lists:mapfoldl(fun(X, C) ->
				  {list_to_tuple(csv_convert([C | X], Types)), C+1} 
			  end, 1, Csv),
    {ok, Csv2, Names, Types, Deps}.


%%%============================================
%%% helper functions
%%%============================================


increase_counter(Ets, Key) ->
    increase_counter(Ets, Key, 1).

increase_counter(Ets, Key, N) ->
    case ets:lookup(Ets, Key) of
	[] ->
	    true = ets:insert_new(Ets, {Key, 0});	    
	_ ->
	    ok
    end,
    ets:update_counter(Ets, Key, N).

str_to_bool([$T|_]) -> true;
str_to_bool([$t|_]) -> true;
str_to_bool([$y|_]) -> true;
str_to_bool([$Y|_]) -> true;

str_to_bool([$F|_]) -> false;
str_to_bool([$f|_]) -> false;
str_to_bool([$n|_]) -> false;
str_to_bool([$N|_]) -> false.


lists_index(Elem, List) ->
    lists_index2(Elem, List, 1).
lists_index2(_Elem, [], _) ->
    false;
lists_index2(Elem, [Elem|_List], N) ->
    N;
lists_index2(Elem, [_|List], N) ->
    lists_index2(Elem, List, N+1).
