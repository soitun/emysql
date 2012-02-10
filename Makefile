all:
	./rebar compile

dist:
	rm -rf rel/emysql
	./rebar generate

clean:
	./rebar clean
