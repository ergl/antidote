%%%% -------------------------------------------------------------------
%%%%
%%%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%%%
%%%% This file is provided to you under the Apache License,
%%%% Version 2.0 (the "License"); you may not use this file
%%%% except in compliance with the License.  You may obtain
%%%% a copy of the License at
%%%%
%%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%%
%%%% Unless required by applicable law or agreed to in writing,
%%%% software distributed under the License is distributed on an
%%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%%% KIND, either express or implied.  See the License for the
%%%% specific language governing permissions and limitations
%%%% under the License.
%%%%
%%%% -------------------------------------------------------------------

-module(rubis).

-define(ru, rubis_utils).
-define(default_group, <<"global_index">>).

-define(committed, {ok, []}).

-type key() :: term().
-type reason() :: atom().

-export([process_request/2]).

sequential_read([], _) ->
    ok;

sequential_read([Key | Rest], Tx) ->
    case pvc:read_single(Key, Tx) of
        {ok, _} ->
            sequential_read(Rest, Tx);
        {error, _}=ReadError ->
            ReadError
    end.

process_request('ByteReq', #{tag := no_op}) ->
    {no_op, os:timestamp()};

process_request('ByteReq', #{tag := ping}) ->
    Start = os:timestamp(),
    {ok, TxId} = pvc:start_transaction(),
    Commit = pvc:commit_transaction(TxId),
    case Commit of
        ?committed ->
            {ping, {Start, os:timestamp()}};
        {error, Reason} ->
            {error, Reason}
    end;

process_request('ByteReq', #{tag := ring}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    CHash = riak_core_ring:chash(Ring),
    Nodes = chash:nodes(CHash),
    Ips = lists:foldl(fun({_, Node}, Acc) ->
        [_, Ip] = binary:split(atom_to_binary(Node, latin1), <<"@">>),
        ordsets:add_element(binary_to_atom(Ip, latin1), Acc)
    end, ordsets:new(), Nodes),
    {ring, Ips};

process_request('TimedRead', #{key := Key}) ->
    Start = os:timestamp(),
    {ok, TxId} = pvc:start_transaction(),
    case pvc:read_single(Key, TxId) of
        {error, ReadReason} ->
            {error, ReadReason};
        {ok, _} ->
            Commit = pvc:commit_transaction(TxId),
            case Commit of
                ?committed ->
                    {ok, {Start, os:timestamp()}};
                {error, Reason} ->
                    {error, Reason}
            end
    end;

process_request('Load', #{num_keys := N, bin_size := Size}) ->
    case pvc:unsafe_load(N, Size) of
        ?committed ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end;

process_request('ReadOnlyTx', #{keys := Keys}) ->
    {ok, TxId} = pvc:start_transaction(),
    case sequential_read(Keys, TxId) of
        {error, _}=ReadError ->
            ReadError;
        ok ->
            Commit = pvc:commit_transaction(TxId),
            case Commit of
                ?committed ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end;

process_request('ReadWriteTx', #{read_keys := Keys, ops := OpList}) ->
    Updates = lists:map(fun(#{key := K, value := V}) -> {K, V} end, OpList),

    {ok, TxId} = pvc:start_transaction(),
    case sequential_read(Keys, TxId) of
        {error, _}=ReadError ->
            ReadError;
        ok ->
            ok = pvc:update_keys(Updates, TxId),
            Commit = pvc:commit_transaction(TxId),
            case Commit of
                ?committed ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end;

%% Used for rubis load
process_request('PutRegion', #{region_name := Name}) ->
    put_region(Name);

%% Used for rubis load
process_request('PutCategory', #{category_name := Name}) ->
    put_category(Name);

%% Benchmark only
process_request('AuthUser', #{username := Username,
                              password := Password}) ->
    auth_user(Username, Password);

%% Used for rubis load and benchmark
process_request('RegisterUser', #{username := Username,
                                  password := Password,
                                  region_id := RegionId}) ->

    register_user(Username, Password, RegionId);

%% Benchmark only
process_request('BrowseCategories', _) ->
    case browse_categories() of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end;

%% Benchmark only
process_request('BrowseRegions', _) ->
    case browse_regions() of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end;

%% Benchmark only
process_request('SearchByCategory', #{category_id := CategoryId}) ->
    case search_items_by_category(CategoryId) of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end;

%% Benchmark only
process_request('SearchByRegion', #{region_id := RegionId, category_id := CategoryId}) ->
    case search_items_by_region(CategoryId, RegionId) of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end;

%% Benchmark only
process_request('ViewItem', #{item_id := ItemId}) ->
    case view_item(ItemId) of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end;

%% Benchmark only
process_request('ViewUser', #{user_id := UserId}) ->
    case view_user(UserId) of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end;

%% Benchmark only
process_request('ViewItemBidHist', #{item_id := ItemId}) ->
    case view_item_bid_hist(ItemId) of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end;

%% Benchmark only
process_request('StoreBuyNow', #{on_item_id := ItemId,
                                 buyer_id := BuyerId,
                                 quantity := Quantity}) ->
    store_buy_now(ItemId, BuyerId, Quantity);

%% Used for rubis load and benchmark
process_request('StoreBid', #{on_item_id := ItemId,
                              bidder_id := BidderId,
                              value := Value}) ->

    store_bid(ItemId, BidderId, Value);

%% Used for rubis load and benchmark
process_request('StoreComment', #{on_item_id := ItemId,
                                  from_id := Fromid,
                                  to_id := ToId,
                                  rating := Rating,
                                  body := Body}) ->

    store_comment(ItemId, Fromid, ToId, Rating, Body);

%% Used for rubis load and benchmark
process_request('StoreItem', #{item_name := Name,
                               description := Desc,
                               quantity := Q,
                               category_id := CategoryId,
                               seller_id := UserId}) ->

    store_item(Name, Desc, Q, CategoryId, UserId);

%% Benchmark only
process_request('AboutMe', #{user_id := UserId}) ->
    case about_me(UserId) of
        {error, Reason} ->
            {error, Reason};
        {ok, _Resp} ->
            ok
    end.

-spec put_region(binary()) -> {ok, key()} | {error, reason()}.
put_region(RegionName) ->
    %% Each region has its own grouping, determined by their region name (unique)
    ChosenPartition = log_utilities:get_key_partition(RegionName),
    Grouping = RegionName,

    %% Generate a new key. A rubis key is wrapped with routing information
    %% (the grouping it should be aggregated with)
    %% All keys under the same grouping will be directed to the same
    %% Antidote partition
    NextRegionId = rubis_keygen_vnode:next_id(ChosenPartition, regions),
    RegionKey = ?ru:gen_key(Grouping, regions, NextRegionId),

    %% We place the region_name index entries in a globally-reachable grouping
    NameIndex = ?ru:gen_index_name(?default_group, regions_name),

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(RegionKey, TxId),
    ok = pvc:update_keys({RegionKey, RegionName}, TxId),
    ok = pvc_indices:index(NameIndex, RegionName, RegionKey, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, RegionKey};
        {error, Reason} ->
            {error, Reason}
    end.

-spec put_category(binary()) -> {ok, key()} | {error, reason()}.
put_category(CategoryName) ->
    %% Each category has its own grouping, determined by their region name (unique)
    ChosenPartition = log_utilities:get_key_partition(CategoryName),
    Grouping = CategoryName,

    %% Generate a new key. A rubis key is wrapped with routing information
    %% (the grouping it should be aggregated with)
    %% All keys under the same grouping will be directed to the same
    %% Antidote partition
    NextRegionId = rubis_keygen_vnode:next_id(ChosenPartition, categories),
    CategoryKey = ?ru:gen_key(Grouping, categories, NextRegionId),

    %% We'll add a global category name index although
    %% it was not in RUBIS originally
    NameIndex = ?ru:gen_index_name(?default_group, categories_name),

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(CategoryKey, TxId),
    ok = pvc:update_keys({CategoryKey, CategoryName}, TxId),
    ok = pvc_indices:index(NameIndex, CategoryName, CategoryKey, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, CategoryKey};
        {error, Reason} ->
            {error, Reason}
    end.

-spec auth_user(binary(), binary()) -> {ok, key()} | {error, reason()}.
auth_user(Username, Password) ->
    NameIndex = ?ru:gen_index_name(?default_group, users_name),
    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, [Val]} = pvc_indices:read_u_index(NameIndex, Username, TxId),
    Result = case Val of
        <<>> ->
            {error, user_not_found};

        UserId ->
            {ok, [{password, Pass}]} = pvc:read_keys(?ru:key_field(UserId, password), TxId),
            case Pass of
                Password ->
                    {ok, UserId};
                _ ->
                    {error, wrong_password}
            end
    end,
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            Result;
        {error, Reason} ->
            {error, Reason}
    end.

-spec register_user(binary(), binary(), key()) -> {ok, key()} | {error, reason()}.
register_user(Username, Password, RegionId) ->
    %% The user_name index live globally to ensure global uniqueness
    UsernameIndex = ?ru:gen_index_name(?default_group, users_name),

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),

    %% Check that our username is unique
    {ok, [Val]} = pvc_indices:read_u_index(UsernameIndex, Username, TxId),
    Result = case Val of
        <<>> ->
            %% Users are grouped into their region name group
            {ok, [RegionName]} = pvc:read_keys(RegionId, TxId),
            RegionPartition = log_utilities:get_key_partition(RegionName),
            SelfGrouping = RegionName,

            %% The user_region index should live along the user itself
            RegionIdIndex = ?ru:gen_index_name(SelfGrouping, users_region),

            NextUserId = rubis_keygen_vnode:next_id(RegionPartition, users),

            UserKey = ?ru:gen_key(SelfGrouping, users, NextUserId),
            UserObj = [
                {UserKey, NextUserId},
                {?ru:gen_key(SelfGrouping, users, NextUserId, username), {username, Username}},
                {?ru:gen_key(SelfGrouping, users, NextUserId, password), {password, Password}},
                {?ru:gen_key(SelfGrouping, users, NextUserId, rating), {rating, 0}},
                {?ru:gen_key(SelfGrouping, users, NextUserId, balance), {balance, 0}},
                {?ru:gen_key(SelfGrouping, users, NextUserId, creationDate), {creationDate, calendar:local_time()}},
                {?ru:gen_key(SelfGrouping, users, NextUserId, regionId), {regionId, RegionId}}
            ],

            ok = pvc:update_keys(UserObj, TxId),
            ok = pvc_indices:u_index(UsernameIndex, Username, UserKey, TxId),
            ok = pvc_indices:index(RegionIdIndex, RegionName, UserKey, TxId),
            {ok, UserKey};

        _ ->
            %% If the username is not unique, abort the transaction
            {error, non_unique_username}
    end,
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            Result;
        {error, Reason} ->
            {error, Reason}
    end.

-spec browse_categories() -> {ok, list()} | {error, reason()}.
browse_categories() ->
    CategoryNameIndex = ?ru:gen_index_name(?default_group, categories_name),
    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, CategoryKeys} = pvc_indices:read_index(CategoryNameIndex, TxId),
    case pvc:read_keys(CategoryKeys, TxId) of
        {error, _}=ReadError ->
            ReadError;

        {ok, Result} ->
            case pvc:commit_transaction(TxId) of
                ?committed ->
                    {ok, Result};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec search_items_by_category(key()) -> {ok, list()} | {error, reason()}.
search_items_by_category(CategoryId) ->
    CategoryGrouping = ?ru:get_grouping(CategoryId),
    CategoryIndex = ?ru:gen_index_name(CategoryGrouping, items_category_id),
    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, ItemKeys} = pvc_indices:read_index(CategoryIndex, CategoryId, TxId),
    case pvc:read_keys(ItemKeys, TxId) of
        {error, _}=ReadError ->
            ReadError;

        {ok, Result} ->
            case pvc:commit_transaction(TxId) of
                ?committed ->
                    {ok, Result};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec browse_regions() -> {ok, list()} | {error, reason()}.
browse_regions() ->
    RegionNameIndex = ?ru:gen_index_name(?default_group, regions_name),
    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, RegionKeys} = pvc_indices:read_index(RegionNameIndex, TxId),
    case pvc:read_keys(RegionKeys, TxId) of
        {error, _}=ReadError ->
            ReadError;

        {ok, Result} ->
            case pvc:commit_transaction(TxId) of
                ?committed ->
                    {ok, Result};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec search_items_by_region(key(), key()) -> {ok, list()} | {error, reason()}.
search_items_by_region(CategoryId, RegionId) ->
    %% Get all items in category CategoryId, such that their sellers
    %% are part of the given region
    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, [RegionName]} = pvc:read_keys(RegionId, TxId),
    UserRegionIndex = ?ru:gen_index_name(RegionName, users_region),
    {ok, UsersInRegion} = pvc_indices:read_index(UserRegionIndex, TxId),
    MatchingItems = lists:flatmap(fun(UserKey) ->
        SellerGroup = ?ru:get_grouping(UserKey),
        SellerIndex = ?ru:gen_index_name(SellerGroup, items_seller_id),
        {ok, SoldByUser} = pvc_indices:read_index(SellerIndex, UserKey, TxId),
        lists:filtermap(fun(ItemKey) ->
            {ok, [{category, ItemCat}]} = pvc:read_keys(?ru:key_field(ItemKey, category), TxId),
            case ItemCat of
                CategoryId ->
                    {true, ItemKey};
                _ ->
                    false
            end
        end, SoldByUser)
    end, UsersInRegion),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, MatchingItems};
        {error, Reason} ->
            {error, Reason}
    end.

-spec view_item(key()) -> {ok, tuple()} | {error, reason()}.
view_item(ItemId) ->
    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, [{seller, SellerId} | ItemDetails]} = pvc:read_keys([?ru:key_field(ItemId, seller),
                                                              ?ru:key_field(ItemId, name),
                                                              ?ru:key_field(ItemId, category),
                                                              ?ru:key_field(ItemId, description),
                                                              ?ru:key_field(ItemId, num_bids),
                                                              ?ru:key_field(ItemId, quantity)], TxId),

    {ok, [SellerUsername,
          SellerRating]} = pvc:read_keys([?ru:key_field(SellerId, username),
                                          ?ru:key_field(SellerId, rating)], TxId),

    ViewItem = {proplists:get_value(name, ItemDetails),
                proplists:get_value(category, ItemDetails),
                proplists:get_value(description, ItemDetails),
                proplists:get_value(num_bids, ItemDetails),
                proplists:get_value(quantity, ItemDetails),
                SellerUsername,
                SellerRating},

    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, ViewItem};
        {error, Reason} ->
            {error, Reason}
    end.

-spec view_user(key()) -> {ok, {binary(), list()}} | {error, reason()}.
view_user(UserId) ->
    UserGroup = ?ru:get_grouping(UserId),
    CommentIndex = ?ru:gen_index_name(UserGroup, comments_to_id),

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, [{username, Username}]} = pvc:read_keys(?ru:key_field(UserId, username), TxId),
    {ok, CommentsToUser} = pvc_indices:read_index(CommentIndex, UserId, TxId),
    CommentInfo = lists:map(fun(CommentId) ->
        {ok, [{from, FromUserId}]} = pvc:read_keys(?ru:key_field(CommentId, from), TxId),
        {ok, [{username, FromUsername}]} = pvc:read_keys(?ru:key_field(FromUserId, username), TxId),
        {CommentId, FromUsername}
    end, CommentsToUser),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, {Username, CommentInfo}};
        {error, Reason} ->
            {error, Reason}
    end.

-spec view_item_bid_hist(key()) -> {ok, {binary(), list()}} | {error, reason()}.
view_item_bid_hist(ItemId) ->
    SelfGrouping = ?ru:get_grouping(ItemId),
    OnItemIdIndex = ?ru:gen_index_name(SelfGrouping, bids_on_item_id),

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, [{name, ItemName}]} = pvc:read_keys(?ru:key_field(ItemId, name), TxId),
    {ok, BidsOnItem} = pvc_indices:read_index(OnItemIdIndex, ItemId, TxId),
    BidInfo = lists:map(fun(BidId) ->
        {ok, [{bidder, UserId}]} = pvc:read_keys(?ru:key_field(BidId, bidder), TxId),
        {ok, [{username, Username}]} = pvc:read_keys(?ru:key_field(UserId, username), TxId),
        {BidId, Username}
    end, BidsOnItem),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, {ItemName, BidInfo}};
        {error, Reason} ->
            {error, Reason}
    end.

-spec store_buy_now(key(), key(), non_neg_integer()) -> {ok, key()} | {error, reason()}.
store_buy_now(OnItemId, BuyerId, Quantity) ->
    %% The grouping for a buy_now object is the items's grouping (OnItemid)
    SelfPartition = log_utilities:get_key_partition(OnItemId),
    SelfGrouping = ?ru:get_grouping(OnItemId),

    %% The grouping for buy_now_buyer_id is the Buyer's group (BuyerId)
    BuyerGrouping = ?ru:get_grouping(BuyerId),
    BuyerIndex = ?ru:gen_index_name(BuyerGrouping, buy_now_buyer_id),

    BuyNowId = rubis_keygen_vnode:next_id(SelfPartition, buy_now),
    BuyNowKey = ?ru:gen_key(SelfGrouping, buy_now, BuyNowId),
    BuyNowObj = [
        {BuyNowKey, BuyNowId},
        {?ru:gen_key(SelfGrouping, buy_now, BuyNowId, bidder), {bidder, BuyerId}},
        {?ru:gen_key(SelfGrouping, buy_now, BuyNowId, on_item), {on_item, OnItemId}},
        {?ru:gen_key(SelfGrouping, buy_now, BuyNowId, quantity), {quantity, Quantity}}
    ],

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),

    %% Insert the buy_now object and update the related index
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(BuyNowKey, TxId),
    ok = pvc:update_keys(BuyNowObj, TxId),
    ok = pvc_indices:index(BuyerIndex, BuyerId, BuyNowKey, TxId),

    %% Update the item quantity
    ItemQuantityKey = ?ru:key_field(OnItemId, quantity),
    {ok, [{quantity, OldQty}]} = pvc:read_keys(ItemQuantityKey, TxId),
    NewQty = case OldQty - Quantity of N when N < 0 -> 0; M -> M end,
    ok = pvc:update_keys({ItemQuantityKey, {quantity, NewQty}}, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, BuyNowKey};
        {error, Reason} ->
            {error, Reason}
    end.

-spec store_bid(key(), key(), non_neg_integer()) -> {ok, key()} | {error, reason()}.
store_bid(OnItemId, BidderId, Value) ->
    %% The grouping for a bid object is the same as the grouping of the item
    SelfPartition = log_utilities:get_key_partition(OnItemId),
    SelfGrouping = ?ru:get_grouping(OnItemId),

    %% The grouping for bids_user is the same as the grouping of the user
    BidderGrouping = ?ru:get_grouping(BidderId),
    BidderIdIndex = ?ru:gen_index_name(BidderGrouping, bids_bidder_id),

    %% The grouping for bids_items is the same as the grouping of the item
    OnItemIdIndex = ?ru:gen_index_name(SelfGrouping, bids_on_item_id),

    BidId = rubis_keygen_vnode:next_id(SelfPartition, bids),
    BidKey = ?ru:gen_key(SelfGrouping, bids, BidId),
    BidObj = [
        {BidKey, BidId},
        {?ru:gen_key(SelfGrouping, bids, BidId, bidder), {bidder, BidderId}},
        {?ru:gen_key(SelfGrouping, bids, BidId, on_item), {on_item, OnItemId}},
        {?ru:gen_key(SelfGrouping, bids, BidId, price), {price, Value}}
    ],

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),

    %% Insert the bid and update the related indices
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(BidKey, TxId),
    %% FIXME(borja): There might be a pvc_bad_vc abort here
    {ok, _} = pvc:read_keys(BidderId, TxId),
    ok = pvc:update_keys(BidObj, TxId),
    ok = pvc_indices:index(OnItemIdIndex, OnItemId, BidKey, TxId),
    ok = pvc_indices:index(BidderIdIndex, BidderId, BidKey, TxId),

    %% Update the referenced item to track the number of bids
    NumBidKey = ?ru:key_field(OnItemId, num_bids),
    MaxBidKey = ?ru:key_field(OnItemId, max_bid),
    {ok, [{num_bids, NBids}, {max_bid, OldMax}]} = pvc:read_keys([NumBidKey, MaxBidKey], TxId),
    NewMax = max(OldMax, Value),
    ok = pvc:update_keys([{NumBidKey, {num_bids, NBids + 1}},
                          {MaxBidKey, {max_bid, NewMax}}], TxId),

    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, BidKey};
        {error, Reason} ->
            {error, Reason}
    end.

-spec store_comment(key(), key(), key(), integer(), binary()) -> {ok, key()} | {error, reason()}.
store_comment(OnItemId, FromId, ToId, Rating, Body) ->
    %% The grouping for a comment object is the poster's grouping (FromId)
    SelfPartition = log_utilities:get_key_partition(FromId),
    SelfGrouping = ?ru:get_grouping(FromId),

    %% The grouping for comments_to_id is the same as the group of to_id
    ToGrouping = ?ru:get_grouping(ToId),
    ToIdIndex = ?ru:gen_index_name(ToGrouping, comments_to_id),

    CommentId = rubis_keygen_vnode:next_id(SelfPartition, comments),

    CommentKey = ?ru:gen_key(SelfGrouping, comments, CommentId),
    CommentObj = [
        {CommentKey, CommentId},
        {?ru:gen_key(SelfGrouping, comments, CommentId, from), {from, FromId}},
        {?ru:gen_key(SelfGrouping, comments, CommentId, to), {to, ToId}},
        {?ru:gen_key(SelfGrouping, comments, CommentId, on_item), {on_item, OnItemId}},
        {?ru:gen_key(SelfGrouping, comments, CommentId, rating), {rating, Rating}},
        {?ru:gen_key(SelfGrouping, comments, CommentId, body), {body, Body}}
    ],

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),

    %% Insert the comment and update the related index
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(CommentKey, TxId),
    ok = pvc:update_keys(CommentObj, TxId),
    ok = pvc_indices:index(ToIdIndex, ToId, CommentKey, TxId),

    %% Update the referenced user to update the rating
    RatingKey = ?ru:key_field(ToId, rating),
    {ok, [{rating, OldRating}]} = pvc:read_keys(RatingKey, TxId),
    NewRating = OldRating + Rating,
    ok = pvc:update_keys({RatingKey, {rating, NewRating}}, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, CommentKey};
        {error, Reason} ->
            {error, Reason}
    end.

-spec store_item(binary(), binary(), non_neg_integer(), key(), key()) -> {ok, key()} | {error, reason()}.
store_item(ItemName, Description, Quantity, CategoryId, SellerId) ->
    %% The item itself has its own grouping, but it is co-located with their seller
    ItemPartition = log_utilities:get_key_partition(SellerId),
    SelfGrouping = ?ru:get_grouping(SellerId),

    %% The items_seller index should live in the seller's group
    SellerIndex = ?ru:gen_index_name(SelfGrouping, items_seller_id),

    %% The item_category index lives in the category's group
    CategoryGrouping = ?ru:get_grouping(CategoryId),
    CategoryIndex = ?ru:gen_index_name(CategoryGrouping, items_category_id),

    ItemId = rubis_keygen_vnode:next_id(ItemPartition, items),
    ItemKey = ?ru:gen_key(SelfGrouping, items, ItemId),
    ItemObj = [
        {ItemKey, ItemId},
        {?ru:gen_key(SelfGrouping, items, ItemId, name), {name, ItemName}},
        {?ru:gen_key(SelfGrouping, items, ItemId, description), {description, Description}},
        {?ru:gen_key(SelfGrouping, items, ItemId, quantity), {quantity, Quantity}},
        {?ru:gen_key(SelfGrouping, items, ItemId, num_bids), {num_bids, 0}},
        {?ru:gen_key(SelfGrouping, items, ItemId, max_bid), {max_bid, 0}},
        {?ru:gen_key(SelfGrouping, items, ItemId, seller), {seller, SellerId}},
        {?ru:gen_key(SelfGrouping, items, ItemId, category), {category, CategoryId}}
    ],

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(ItemKey, TxId),
    ok = pvc:update_keys(ItemObj, TxId),
    ok = pvc_indices:index(CategoryIndex, CategoryId, ItemKey, TxId),
    ok = pvc_indices:index(SellerIndex, SellerId, ItemKey, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, ItemKey};
        {error, Reason} ->
            {error, Reason}
    end.

-spec about_me(key()) -> {ok, binary(), [any()]} | {error, reason()}.
about_me(UserId) ->
    UserGroup = ?ru:get_grouping(UserId),
    SellerIndex = ?ru:gen_index_name(UserGroup, items_seller_id),

    {ok, TxId} = pvc:start_transaction(),
    %% lager:info("{~p} ~p", [erlang:phash2(TxId), ?FUNCTION_NAME]),
    {ok, [{username, Username}]} = pvc:read_keys(?ru:key_field(UserId, username), TxId),

    %% Get all the items sold by the given UserId
    {ok, SoldItems} = pvc_indices:read_index(SellerIndex, UserId, TxId),
    {ok, _ItemInfo} = pvc:read_keys(SoldItems, TxId),

    UserDetails = case get_bought_items(UserId, TxId) of
        {error, _}=Err0 ->
            Err0;

        {ok, _BoughtInfo} ->
            case get_bidded_items(UserId, TxId) of
                {error, _}=Err1 ->
                    Err1;

                {ok, _BidInfo} ->
                    case get_authored_comments(UserId, TxId) of
                        {error, _}=Err2 ->
                            Err2;

                        {ok, _AuthoredComments} ->
                            ok
                    end
            end
    end,

    case UserDetails of
        {error, _}=DetailsErr ->
            DetailsErr;

        ok ->
            case pvc:commit_transaction(TxId) of
                ?committed ->
                    {ok, Username};

                {error, _}=CommitErr ->
                    CommitErr
            end

    end.

%% Get all the buy_now actions performed by the given UserId,
%% along with the item info, and the username of the seller
get_bought_items(UserId, TxId) ->
    UserGroup = ?ru:get_grouping(UserId),
    BuyerIndex = ?ru:gen_index_name(UserGroup, buy_now_buyer_id),
    {ok, Bought} = pvc_indices:read_index(BuyerIndex, UserId, TxId),
    map_error(fun(BuyNowId) ->
        OnItemKey = ?ru:key_field(BuyNowId, on_item),
        QuantityKey = ?ru:key_field(BuyNowId, quantity),
        case pvc:read_keys([OnItemKey, QuantityKey], TxId) of
            {error, _}=Err ->
                throw(Err);

            {ok, [{on_item, OnItemId}, {quantity, Quantity}]} ->
                {ok, [{seller, SellerId},
                      {name, ItemName}]} = pvc:read_keys([?ru:key_field(OnItemId, seller),
                                                          ?ru:key_field(OnItemId, name)], TxId),

                {ok, [{username, SellerUsername}]} = pvc:read_keys(?ru:key_field(SellerId, username), TxId),
                {SellerUsername, ItemName, Quantity}
        end
    end, Bought).

%% Get all the bids performed by the given UserId,
%% along with the item info, and the username of the seller
get_bidded_items(UserId, TxId) ->
    UserGroup = ?ru:get_grouping(UserId),
    BidderIndex = ?ru:gen_index_name(UserGroup, bids_bidder_id),
    {ok, PlacedBids} = pvc_indices:read_index(BidderIndex, UserId, TxId),
    map_error(fun(BidId) ->
        case pvc:read_keys(?ru:key_field(BidId, on_item), TxId) of
            {error, _}=Err ->
                throw(Err);

            {ok, [{on_item, OnItemId}]} ->
                {ok, [{seller, SellerId}]} = pvc:read_keys(?ru:key_field(OnItemId, seller), TxId),
                {ok, [{username, SellerUsername}]} = pvc:read_keys(?ru:key_field(SellerId, username), TxId),
                {OnItemId, SellerUsername}
        end
    end, PlacedBids).

%% Get all the comments authored by the given UserId
get_authored_comments(UserId, TxId) ->
    UserGroup = ?ru:get_grouping(UserId),
    CommentIndex = ?ru:gen_index_name(UserGroup, comments_to_id),
    {ok, CommentsToUser} = pvc_indices:read_index(CommentIndex, UserId, TxId),
    pvc:read_keys(CommentsToUser, TxId).

%% Util functions

-spec map_error(fun((any()) -> any()), [any()]) -> {ok, [any()]} | {error, term()}.
map_error(Fun, List) ->
    try
        {ok, lists:map(Fun, List)}
    catch {error, pvc_bad_vc} ->
        {error, pvc_bad_vc}
    end.
