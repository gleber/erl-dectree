-module(log).

-compile(export_all).

%-define(debug, true).

-ifdef(debug).
-define(log(Msg, Args), 
	io:format("~p: Log: " ++ Msg ++"~n", ([self() | Args]))
       ).
-define(logs(Msg), io:format("~p: Log: " ++ Msg ++ "~n", [self()])).
-else.
-define(log(Msg, Args), true).
-define(logs(Msg), true).
-endif.

log(Msg) ->
    ?logs(Msg).
log(Msg, Args) ->
    ?log(Msg, Args).
