-module(kvs_meeting).
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/meetings.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("kvs/include/log.hrl").
-compile(export_all).

create_team(Name) ->
    TID = kvs:next_id("team",1),
    ok = kvs:put(Team = #team{id=TID,name=Name}),
    TID.

create(UID, Name) -> create(UID, Name, "", date(), time(), 100, 100, undefined, pointing, game_okey, standard, 8, slow).
create(UID, Name, Desc, Date, Time, Players, Quota, Awards, Type, Game, Mode, Tours, Speed) ->
    NodeAtom = nsx_opt:get_env(store,game_srv_node,'game@doxtop.cc'),
    TID = rpc:call(NodeAtom, game_manager, gen_game_id, []),
    CTime = erlang:now(),
    kvs:put(#meeting{name = Name, id = TID, description = Desc, quota = Quota,
        players_count = Players, start_date = Date, awards = Awards,
        creator = UID, created = CTime, game_type = Game, game_mode = Mode,
        type = Type, tours = Tours, speed = Speed, start_time = Time, status = created, owner = UID}),
    TID.

get(TID) ->
    case kvs:get(meeting, TID) of
        {ok, Tournament} -> Tournament;
        {error, not_found} -> #meeting{};
        {error, notfound} -> #meeting{}
    end.

start(_TID) -> ok.
join(UID, TID) -> kvs:join_tournament(UID, TID).
remove(UID, TID) -> kvs:leave_tournament(UID, TID).
waiting_player(TID) -> kvs:tournament_pop_waiting_player(TID).
joined_users(TID) -> kvs:tournament_waiting_queue(TID).
user_joined(TID, UID) -> 
    AllJoined = [UId || #play_record{who = UId} <- joined_users(TID)],
    lists:member(UID, AllJoined).
all() -> kvs:all(tournament).
user_is_team_creator(_UID, _TID) -> true.
list_users_per_team(_TeamID) -> [].
destroy(TID) -> kvs:delete_by_index(play_record, <<"play_record_tournament_bin">>, TID),
                          kvs:delete(tournament,TID).
clear() -> [destroy(T#meeting.id) || T <- kvs:all(meeting)].
lost() -> lists:usort([erlang:element(3, I) || I <- kvs:all(play_record)]).
fake_join(TID) -> [kvs_meeting:join(auth:ima_gio2(X),TID)||X<-lists:seq(1,30)].

handle_notice(["kvs_meeting", "user", UId, "create"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): create: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType} = Message,
    case kvs_meeting:create(UId, TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType) of
        {error,X} -> 
            ?ERROR("Error creating tournament: ~p", X);
        TId -> skip end,
    {noreply, State};

handle_notice(["kvs_meeting", "user", UId, "create_and_join"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): create_and_join: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType} = Message,
    case kvs_meeting:create(UId, TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType) of
        {error,X} -> 
            ?ERROR("Error creating tournament: ~p", X);
        TId -> kvs_meeting:join(UId, TId) end,
    {noreply, State};

handle_notice(["kvs_meeting", "user", UId, "join"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): tournament_join: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, TId} = Message,
    kvs_meeting:join(UId, TId),
    {noreply, State};

handle_notice(["kvs_meeting", "user", UId, "leave"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): tournament_remove: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, TId} = Message,
    kvs_meeting:remove(UId, TId),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown MEETINGS notice").

join_tournament(UserId, TournamentId) ->
    case kvs:get(user, UserId) of
        {ok, User} ->
            GP = case kvs_account:balance(UserId, points) of
                     {ok, AS1} -> AS1;
                     {error, _} -> 0 end,
            Q = case kvs_account:balance(UserId,  quota) of
                     {ok, AS4} -> AS4;
                     {error, _} -> 0 end,
            RN = kvs_users:user_realname(UserId),
            kvs:put(#play_record{
                 who = UserId,
                 tournament = TournamentId,
                 team = User#user.team,
                 game_id = undefined, 
		 other = now(),
                 realname = RN,
                 points = GP,
                 quota = Q});
        _ ->
            ?INFO(" User ~p not found for joining tournament ~p", [UserId, TournamentId])
    end.

leave_tournament(UserId, TournamentId) ->
    case kvs:get(play_record, {UserId, TournamentId}) of
        {ok, _} -> 
            kvs:delete(play_record, {UserId, TournamentId}),
            leave_tournament(UserId, TournamentId); % due to WTF error with old records
        _ -> ok
    end.

user_tournaments(UId) -> 
    kvs:all_by_index(play_record, <<"play_record_who_bin">>, list_to_binary(UId)).

tournament_waiting_queue(TId) ->
    kvs:all_by_index(play_record, <<"play_record_tournament_bin">>, list_to_binary(integer_to_list(TId))).
