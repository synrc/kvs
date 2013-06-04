-define(LOG(Format, Args, Level, Tags), error_logger:info_msg("~p:~p " ++ Format,[?MODULE,?LINE] ++ Args)).
-define(WRN(Format, Args, Level, Tags), error_logger:warning_msg("~p:~p " ++ Format,[?MODULE,?LINE] ++ Args)).
-define(ERR(Format, Args, Level, Tags), error_logger:error_msg("~p:~p " ++ Format,[?MODULE,?LINE] ++ Args)).

-define(INFO(Format, Args, Tag), ?LOG(Format, Args, ?info, Tag)).
-define(INFO(Format, Args),      ?INFO(Format, Args, [])).
-define(INFO(Format),            ?INFO(Format, [])).

-define(WARNING(Format, Args, Tag), ?WRN(Format, Args, ?warning, Tag)).
-define(WARNING(Format, Args),      ?WARNING(Format, Args, [])).
-define(WARNING(Format),            ?WARNING(Format, [])).

-define(ERROR(Format, Args, Tag), ?ERR(Format, Args, ?error, Tag)).
-define(ERROR(Format, Args),      ?ERROR(Format, Args, [])).
-define(ERROR(Format),            ?ERROR(Format, [])).

