-module(kvs_dbs).
-include_lib("kvs/include/test.hrl").
-include_lib("kvs/include/metainfo.hrl").

%% API
-export([metainfo/0]).

metainfo() ->
  #schema
  {
    name=kvs,
    tables=
      [
        #table
        {
          name = test,
          fields = record_info(fields, test),
          columns =
            [
              #column{name = <<"id">>, type = varchar, key = true, ro = false},
              #column{name = <<"version">>, type = varchar},
              #column{name = <<"container">>, type = varchar},
              #column{name = <<"feed_id">>, type = varchar},
              #column{name = <<"prev">>, type = varchar},
              #column{name = <<"next">>, type = varchar},
              #column{name = <<"feeds">>, type = varchar},
              #column{name = <<"guard">>, type = varchar},
              #column{name = <<"etc">>, type = varchar},
              #column{name = <<"data">>, type = varchar}
            ]
        },
        #table
        {

        }
      ]
  }.
