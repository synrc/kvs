-module(kvs_meeting).
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/meetings.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("kvs/include/config.hrl").
-compile(export_all).

init(Backend) ->
    ?CREATE_TAB(team),
    ok.

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
        {error, _} -> #meeting{} end.

start(_TID) -> ok.
join(UID, TID) -> join_tournament(UID, TID).
leave(UID, TID) -> leave_tournament(UID, TID).
user_joined(TID, UID) -> 
    AllJoined = [UId || #play_record{who = UId} <- tournament_users(TID)],
    lists:member(UID, AllJoined).
all() -> kvs:all(tournament).
user_is_team_creator(_UID, _TID) -> true.
destroy(TID) -> kvs:delete_by_index(play_record, <<"play_record_tournament_bin">>, TID),
                          kvs:delete(tournament,TID).
clear() -> [destroy(T#meeting.id) || T <- kvs:all(meeting)].
lost() -> lists:usort([erlang:element(3, I) || I <- kvs:all(play_record)]).

handle_notice(["kvs_meeting", "user", UId, "create"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): create: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType} = Message,
    case kvs_meeting:create(UId, TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType) of
        {error,X} -> 
            error_logger:info_msg("Error creating tournament: ~p", X);
        TId -> skip end,
    {noreply, State};

handle_notice(["kvs_meeting", "user", UId, "create_and_join"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): create_and_join: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType} = Message,
    case kvs_meeting:create(UId, TourName, TourDesc, {Y,M,D}, Time, MaxPlayers, Quota, Award, TourType, GameType) of
        {error,X} -> 
            error_logger:info_msg("Error creating tournament: ~p", X);
        TId -> kvs_meeting:join(UId, TId) end,
    {noreply, State};

handle_notice(["kvs_meeting", "user", UId, "join"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): tournament_join: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, TId} = Message,
    kvs_meeting:join(UId, TId),
    {noreply, State};

handle_notice(["kvs_meeting", "user", UId, "leave"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): tournament_remove: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, TId} = Message,
    kvs_meeting:leave(UId, TId),
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
                 realname = RN,
                 points = GP,
                 quota = Q});
        _ ->
            error_logger:info_msg(" User ~p not found for joining tournament ~p", [UserId, TournamentId])
    end.

leave_tournament(UserId, TournamentId) ->
    case kvs:get(play_record, {UserId, TournamentId}) of
        {ok, _} -> 
            kvs:delete(play_record, {UserId, TournamentId}),
            leave_tournament(UserId, TournamentId); % due to WTF error with old records
        _ -> ok
    end.

user_tournaments(UId) -> DBA=?DBA,DBA:user_tournaments(UId).
tournament_users(TId) -> DBA=?DBA,DBA:tournament_users(TId).
