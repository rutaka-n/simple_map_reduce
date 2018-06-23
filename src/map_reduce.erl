-module(map_reduce).

%% API exports
-export([start/1]).

%%====================================================================
%% API functions
%%====================================================================

start(FileNames) ->
        do_map_reduce(FileNames).

%%====================================================================
%% Internal functions
%%====================================================================

do_map(FileName, Pid) ->
        case file:open(FileName, [read, binary, {encoding, utf8}]) of
                {ok, IO} ->
                        Res = process_file(IO, dict:new()),
                        file:close(IO),
                        Pid ! {self(), Res};
                {error, _} -> Pid ! {self(), error}
        end.

process_file(IO, Acc) ->
        case io:get_line(IO, []) of
                eof -> Acc;
                Line ->
                        Words = split_by_words(Line),
                        process_file(IO, do_count(Words, Acc))
        end.

do_count(Words, Acc) ->
        lists:foldl(fun(Word, Ac) ->
                                    dict:update_counter(Word, 1, Ac)
                    end, Acc, Words).

split_by_words(Line) ->
        re:split(Line,"\\s", [unicode, {return,list}]).

do_map_reduce(FileNames) ->
        Pid = self(),
        do_reduce([spawn_link(fun() -> do_map(FileName, Pid) end) || FileName <- FileNames], maps:new()).

do_reduce([], Result) -> Result;
do_reduce(Pids, Result) ->
        receive
                {Pid, error} ->
                        do_reduce(lists:delete(Pid, Pids), Result);
                {Pid, Dict} ->
                        UpdatedResult = lists:foldr(
                                          fun({Word, Amount}, Acc) ->
                                                          maps:put(Word, Amount + maps:get(Word, Acc, 0),Acc)
                                          end,
                                          Result,
                                          dict:to_list(Dict)
                                         ),
                        do_reduce(lists:delete(Pid, Pids), UpdatedResult)
        end.
