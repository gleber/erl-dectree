-module(gui).

-export([run/0,
	 run_child/0,
	 run_net/0]).

%%%============================================
%%% GUI stuff
%%%============================================

do_dist_file(FN) ->
    File = dec_tree:csv_filter(csv:read(dec_tree:readfile(FN))),
    {ok, {Csv, _Size}, Names, Types, Deps} = dataset_ets:csv_process(File),
    data_server:init(Names, Types, Deps),
    data_server:set_child_data(Csv),
    {_Time, T} = timer:tc(data_server, distributed_create_tree, []),
    T2 = dec_tree:prune_tree(T),
    %%write_tree(T2),
    {T2, T, Csv}.


get_line(Prompt) ->
    S = io:get_line(Prompt),
    string:strip(string:strip(S, both, hd("\n"))).


process_file_menu() ->
    {ok, Dir} = file:get_cwd(),
    io:fwrite("Current directory: ~s~n", [Dir]),
    FN = get_line('Enter filename: '),
    {T, _} = file:read_file_info(FN),
    case T of
	ok ->
	    data_server:new(),
	    Tree = do_dist_file(FN),
	    tree_menu(Tree),
	    data_server:stop();
	error ->
	    io:fwrite("File does not exists~n"),
	    main_menu()
    end.

tree_menu({Tree, UnpTree, Dataset_Ets} = State) ->
    io:fwrite("1. Print tree~n"),
    io:fwrite("2. Print unpruned tree~n"),
    io:fwrite("3. Test tree on original data~n"),
    io:fwrite("0. Back~n"),
    case get_line('> ') of
	eof ->
	    ok;
	"1" ->
	    dec_tree:write_tree(Tree),
	    tree_menu(State);
	"2" ->
	    dec_tree:write_tree(UnpTree),
	    tree_menu(State);
	"3" ->
	    {Count, Good} = dec_tree:classify_dataset_ets(Dataset_Ets, Tree),
	    io:fwrite("~p of ~p correct classified~n", [Good, Count]),
	    tree_menu(State);
	"0" ->
	    ok;
	Other ->
	    io:fwrite("Unrecognized command: ~p~n", [Other]),
	    tree_menu(State)
    end.    

main_menu() ->
    io:fwrite("~n"),
    io:fwrite("1. Create tree~n"),
    io:fwrite("2. List nodes~n"),
    io:fwrite("0. Quit~n"),
    case get_line('> ') of
	eof ->
	    ok;
	"1" ->
	    process_file_menu(),
	    main_menu();
	"2" ->
	    io:fwrite("Nodes: ~p", [[node() | nodes()]]),
	    main_menu();
	"0" ->
	    ok;
	Other ->
	    io:fwrite("Unrecognized command: ~p~n", [Other]),
	    main_menu()	  
    end.
	

load_cluster() ->
%%    make:all([load]),
    code:add_path("ebin"),
    io:fwrite("Nodes: ~p~n", [[node() | nodes()]]),
    L = [csv, dataset, log, dataset_ets, data_server, dec_tree, plists],
    lists:map(fun(M) ->
		      %%io:fwrite("Loading ~p~n", [M]),
		      {_M, B, F} = code:get_object_code(M), 
		      rpc:multicall(code, load_binary, [M, F, B])
	      end, L).
    %%io:fwrite("Success~n").

run_child() ->
    io:fwrite("Input node name~n"),
    case get_line('> ') of
	eof ->
	    ok;
	M ->
	    net_kernel:start([list_to_atom(M)]),
	    net_adm:world(),
	    child_run()
    end.

child_run() ->
    io:fwrite("Type \"quit\" to quit"),
    case get_line('> ') of
	eof ->
	    ok;
	"quit" ->
	    ok;
	_ ->
	    child_run()
    end.   
    

run_net() ->
    net_kernel:start([list_to_atom("master")]),
    net_adm:world(),
    run().
	   

run() ->
    load_cluster(),
    main_menu().

