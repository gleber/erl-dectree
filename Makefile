compile:
	erl -make

run:
	erl -noshell -pa ebin/ -s gui run -s erlang halt

run_master:
	erl -noshell -pa ebin/ -s gui run_net -s erlang halt

run_child:
	erl -noshell -pa ebin/ -s gui run_child -s erlang halt
