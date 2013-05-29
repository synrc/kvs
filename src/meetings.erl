-module(meetings).
-include("users.hrl").
-include("meetings.hrl").
-compile(export_all).

create_team(Name) ->
    TID = store:next_id("team",1),
    ok = store:put(Team = #team{id=TID,name=Name}),
    TID.

create(UID, Name) -> create(UID, Name, "", date(), time(), 100, 100, undefined, pointing, game_okey, standard, 8, slow).
create(UID, Name, Desc, Date, Time, Players, Quota, Awards, Type, Game, Mode, Tours, Speed) ->
    NodeAtom = nsx_opt:get_env(store,game_srv_node,'game@doxtop.cc'),
    TID = rpc:call(NodeAtom, game_manager, gen_game_id, []),

    CTime = erlang:now(),
    ok = store:put(#tournament{name = Name,
                                   id = TID,
                                   description = Desc,
                                   quota = Quota,
                                   players_count = Players,
                                   start_date = Date,
                                   awards = Awards,
                                   creator = UID,
                                   created = CTime,
                                   game_type = Game,
                                   game_mode = Mode,
                                   type = Type,
                                   tours = Tours,
                                   speed = Speed,
                                   start_time = Time,
                                   status = created,
                                   owner = UID}),

    TID.

get(TID) ->
    case store:get(tournament, TID) of
        {ok, Tournament} -> Tournament;
        {error, not_found} -> #tournament{};
        {error, notfound} -> #tournament{}
    end.

start(_TID) -> ok.
join(UID, TID) -> store:join_tournament(UID, TID).
remove(UID, TID) -> store:leave_tournament(UID, TID).
waiting_player(TID) -> store:tournament_pop_waiting_player(TID).
joined_users(TID) -> store:tournament_waiting_queue(TID).
user_tournaments(UID) -> store:user_tournaments(UID).
user_joined(TID, UID) -> 
    AllJoined = [UId || #play_record{who = UId} <- joined_users(TID)],
    lists:member(UID, AllJoined).
all() -> store:all(tournament).
user_is_team_creator(_UID, _TID) -> true.
list_users_per_team(_TeamID) -> [].
destroy(TID) -> store:delete_by_index(play_record, <<"play_record_tournament_bin">>, TID),
                          store:delete(tournament,TID).
clear() -> [destroy(T#tournament.id) || T <- store:all(tournament)].
lost() -> lists:usort([erlang:element(3, I) || I <- store:all(play_record)]).
fake_join(TID) -> [nsm_tournaments:join(nsm_auth:ima_gio2(X),TID)||X<-lists:seq(1,30)].
