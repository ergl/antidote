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

-record(user, {
    username :: binary(),
    password :: binary(),
    rating :: integer(),
    balance :: integer(),
    creationDate :: calendar:datetime(),
    regionId :: key()
}).

-record(item, {
    name :: binary(),
    description :: binary(),
    quantity :: non_neg_integer(),
    num_bids :: non_neg_integer(),
    max_bid :: non_neg_integer(),
    seller :: key(),
    category :: key()
}).

-record(bid, {
    bidder :: key(),
    on_item :: key(),
    price :: non_neg_integer()
}).

-record(buy_now, {
    bidder :: key(),
    on_item :: key(),
    quantity :: non_neg_integer()
}).

-record(comment, {
    from :: key(),
    to :: key(),
    on_item :: key(),
    rating :: integer(),
    body :: binary()
}).

-record(view_item, {
    item_category :: key(),
    item_description :: binary(),
    item_name :: binary(),
    item_num_bids :: non_neg_integer(),
    item_quantity :: non_neg_integer(),
    user_username :: binary(),
    user_rating :: integer()
}).

-export([process_request/2]).

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
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
            ok
    end;

%% Benchmark only
process_request('BrowseRegions', _) ->
    case browse_regions() of
        {error, Reason} ->
            {error, Reason};
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
            ok
    end;

%% Benchmark only
process_request('SearchByCategory', #{category_id := CategoryId}) ->
    case search_items_by_category(CategoryId) of
        {error, Reason} ->
            {error, Reason};
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
            ok
    end;

%% Benchmark only
process_request('SearchByRegion', #{region_id := RegionId, category_id := CategoryId}) ->
    case search_items_by_region(CategoryId, RegionId) of
        {error, Reason} ->
            {error, Reason};
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
            ok
    end;

%% Benchmark only
process_request('ViewItem', #{item_id := ItemId}) ->
    case view_item(ItemId) of
        {error, Reason} ->
            {error, Reason};
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
            ok
    end;

%% Benchmark only
process_request('ViewUser', #{user_id := UserId}) ->
    case view_user(UserId) of
        {error, Reason} ->
            {error, Reason};
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
            ok
    end;

%% Benchmark only
process_request('ViewItemBidHist', #{item_id := ItemId}) ->
    case view_item_bid_hist(ItemId) of
        {error, Reason} ->
            {error, Reason};
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
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
        {ok, Resp} ->
            lager:info("Got result ~p", [Resp]),
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
    {ok, [Val]} = pvc_indices:read_u_index(NameIndex, Username, TxId),
    Result = case Val of
        <<>> ->
            {error, user_not_found};

        UserId ->
            {ok, [#user{password = Pass}]} = pvc:read_keys(UserId, TxId),
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

            UserObj = #user{username = Username,
                            password = Password,
                            rating = 0,
                            balance = 0,
                            creationDate = calendar:local_time(),
                            regionId = RegionId},

            ok = pvc:update_keys({UserKey, UserObj}, TxId),
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
    {ok, CategoryKeys} = pvc_indices:read_index(CategoryNameIndex, TxId),
    Result = pvc:read_keys(CategoryKeys, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            Result;
        {error, Reason} ->
            {error, Reason}
    end.

-spec search_items_by_category(key()) -> {ok, list()} | {error, reason()}.
search_items_by_category(CategoryId) ->
    CategoryGrouping = ?ru:get_grouping(CategoryId),
    CategoryIndex = ?ru:gen_index_name(CategoryGrouping, items_category_id),
    {ok, TxId} = pvc:start_transaction(),
    {ok, ItemKeys} = pvc_indices:read_index(CategoryIndex, CategoryId, TxId),
    Result = pvc:read_keys(ItemKeys, TxId),
    {ok, []} = pvc:commit_transaction(TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            Result;
        {error, Reason} ->
            {error, Reason}
    end.

-spec browse_regions() -> {ok, list()} | {error, reason()}.
browse_regions() ->
    RegionNameIndex = ?ru:gen_index_name(?default_group, regions_name),
    {ok, TxId} = pvc:start_transaction(),
    {ok, RegionKeys} = pvc_indices:read_index(RegionNameIndex, TxId),
    Result = pvc:read_keys(RegionKeys, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            Result;
        {error, Reason} ->
            {error, Reason}
    end.

-spec search_items_by_region(key(), key()) -> {ok, list()} | {error, reason()}.
search_items_by_region(CategoryId, RegionId) ->
    %% Get all items in category CategoryId, such that their sellers
    %% are part of the given region
    {ok, TxId} = pvc:start_transaction(),
    {ok, [RegionName]} = pvc:read_keys(RegionId, TxId),
    UserRegionIndex = ?ru:gen_index_name(RegionName, users_region),
    {ok, UsersInRegion} = pvc_indices:read_index(UserRegionIndex, TxId),
    MatchingItems = lists:flatmap(fun(UserKey) ->
        SellerGroup = ?ru:get_grouping(UserKey),
        SellerIndex = ?ru:gen_index_name(SellerGroup, items_seller_id),
        {ok, SoldByUser} = pvc_indices:read_index(SellerIndex, UserKey, TxId),
        lists:filtermap(fun(ItemKey) ->
            {ok, [Item]} = pvc:read_keys(ItemKey, TxId),
            case Item#item.category of
                CategoryId ->
                    {true, Item};
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

-spec view_item(key()) -> {ok, #view_item{}} | {error, reason()}.
view_item(ItemId) ->
    {ok, TxId} = pvc:start_transaction(),
    {ok, [Item]} = pvc:read_keys(ItemId, TxId),
    SellerId = Item#item.seller,
    {ok, [Seller]} = pvc:read_keys(SellerId, TxId),
    ViewItem = #view_item{item_name = Item#item.name,
                          item_category = Item#item.category,
                          item_description = Item#item.description,
                          item_num_bids = Item#item.num_bids,
                          item_quantity = Item#item.quantity,
                          user_username = Seller#user.username,
                          user_rating = Seller#user.rating},
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
    {ok, [#user{username = Username}]} = pvc:read_keys(UserId, TxId),
    {ok, CommentsToUser} = pvc_indices:read_index(CommentIndex, UserId, TxId),
    CommentInfo = lists:map(fun(CommentId) ->
        {ok, [Comment = #comment{from = FromUserId}]} = pvc:read_keys(CommentId, TxId),
        {ok, [#user{username = FromUsername}]} = pvc:read_keys(FromUserId, TxId),
        {Comment, FromUsername}
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
    {ok, [#item{name = ItemName}]} = pvc:read_keys(ItemId, TxId),
    {ok, BidsOnItem} = pvc_indices:read_index(OnItemIdIndex, ItemId, TxId),
    BidInfo = lists:map(fun(BidId) ->
        {ok, [Bid = #bid{bidder = UserId}]} = pvc:read_keys(BidId, TxId),
        {ok, [#user{username = Username}]} = pvc:read_keys(UserId, TxId),
        {Bid, Username}
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
    BuyNowObj = #buy_now{bidder = BuyerId,
                         on_item = OnItemId,
                         quantity = Quantity},

    {ok, TxId} = pvc:start_transaction(),

    %% Insert the buy_now object and update the related index
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(BuyNowKey, TxId),
    ok = pvc:update_keys({BuyNowKey, BuyNowObj}, TxId),
    ok = pvc_indices:index(BuyerIndex, BuyerId, BuyNowKey, TxId),

    %% Update the item quantity
    {ok, [Item]} = pvc:read_keys(OnItemId, TxId),
    OldQty = Item#item.quantity,
    NewQty = case OldQty - Quantity of N when N < 0 -> 0; M -> M end,
    UpdatedItem = Item#item{quantity = NewQty},
    ok = pvc:update_keys({OnItemId, UpdatedItem}, TxId),
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
    BidObj = #bid{bidder = BidderId,
                  on_item = OnItemId,
                  price = Value},

    {ok, TxId} = pvc:start_transaction(),

    %% Insert the bid and update the related indices
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(BidKey, TxId),
    ok = pvc:update_keys({BidKey, BidObj}, TxId),
    ok = pvc_indices:index(OnItemIdIndex, OnItemId, BidKey, TxId),
    ok = pvc_indices:index(BidderIdIndex, BidderId, BidKey, TxId),

    %% Update the referenced item to track the number of bids
    {ok, [Item]} = pvc:read_keys(OnItemId, TxId),
    NBids = Item#item.num_bids,
    NewMax = max(Item#item.max_bid, Value),
    UpdatedItem = Item#item{num_bids = NBids + 1, max_bid = NewMax},
    ok = pvc:update_keys({OnItemId, UpdatedItem}, TxId),
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
    CommentObj = #comment{from = FromId,
                          to = ToId,
                          on_item = OnItemId,
                          rating = Rating,
                          body = Body},

    {ok, TxId} = pvc:start_transaction(),

    %% Insert the comment and update the related index
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(CommentKey, TxId),
    ok = pvc:update_keys({CommentKey, CommentObj}, TxId),
    ok = pvc_indices:index(ToIdIndex, ToId, CommentKey, TxId),

    %% Update the referenced user to update the rating
    {ok, [User]} = pvc:read_keys(ToId, TxId),
    OldRating = User#user.rating,
    UpdatedUser = User#user{rating = OldRating + Rating},
    ok = pvc:update_keys({ToId, UpdatedUser}, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, CommentKey};
        {error, Reason} ->
            {error, Reason}
    end.

-spec store_item(binary(), binary(), non_neg_integer(), key(), key()) -> {ok, key()} | {error, reason()}.
store_item(ItemName, Description, Quantity, CategoryId, SellerId) ->
    %% The item itself has its own grouping, determined by their item name
    ItemPartition = log_utilities:get_key_partition(ItemName),
    SelfGrouping = ItemName,

    %% The items_seller index should live in the seller's group
    SellerGrouping = ?ru:get_grouping(SellerId),
    SellerIndex = ?ru:gen_index_name(SellerGrouping, items_seller_id),

    %% The item_category index lives in the category's group
    CategoryGrouping = ?ru:get_grouping(CategoryId),
    CategoryIndex = ?ru:gen_index_name(CategoryGrouping, items_category_id),

    ItemId = rubis_keygen_vnode:next_id(ItemPartition, items),
    ItemKey = ?ru:gen_key(SelfGrouping, items, ItemId),
    ItemObj = #item{name = ItemName,
                    description = Description,
                    quantity = Quantity,
                    num_bids = 0,
                    max_bid = 0,
                    seller = SellerId,
                    category = CategoryId},

    {ok, TxId} = pvc:start_transaction(),
    %% Make sure we don't perform blind-writes
    {ok, [<<>>]} = pvc:read_keys(ItemKey, TxId),
    ok = pvc:update_keys({ItemKey, ItemObj}, TxId),
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
    BidderIndex = ?ru:gen_index_name(UserGroup, bids_bidder_id),
    CommentIndex = ?ru:gen_index_name(UserGroup, comments_to_id),
    BuyerIndex = ?ru:gen_index_name(UserGroup, buy_now_buyer_id),

    {ok, TxId} = pvc:start_transaction(),
    {ok, [#user{username = Username}]} = pvc:read_keys(UserId, TxId),

    %% Get all the items sold by the given UserId
    {ok, SoldItems} = pvc_indices:read_index(SellerIndex, UserId, TxId),
    {ok, ItemInfo} = pvc:read_keys(SoldItems, TxId),

    %% Get all the buy_now actions performed by the given UserId,
    %% along with the item info, and the username of the seller
    {ok, Bought} = pvc_indices:read_index(BuyerIndex, UserId, TxId),
    BoughtInfo = lists:map(fun(BuyNowId) ->
        {ok, [BuyNowObj = #buy_now{on_item = OnItemId}]} = pvc:read_keys(BuyNowId, TxId),
        {ok, [OnItem = #item{seller = SellerId}]} = pvc:read_keys(OnItemId, TxId),
        {ok, [#user{username = SellerUsername}]} = pvc:read_keys(SellerId, TxId),
        {BuyNowObj, OnItem, SellerUsername}
    end, Bought),

    %% Get all the bids performed by the given UserId,
    %% along with the item info, and the username of the seller
    {ok, PlacedBids} = pvc_indices:read_index(BidderIndex, UserId, TxId),
    BidInfo = lists:map(fun(BidId) ->
        {ok, [#bid{on_item = OnItemId}]} = pvc:read_keys(BidId, TxId),
        {ok, [OnItem = #item{seller = SellerId}]} = pvc:read_keys(OnItemId, TxId),
        {ok, [#user{username = SellerUsername}]} = pvc:read_keys(SellerId, TxId),
        {OnItem, SellerUsername}
    end, PlacedBids),

    %% Get all the comments authored by the given UserId
    {ok, CommentsToUser} = pvc_indices:read_index(CommentIndex, UserId, TxId),
    {ok, CommentInfo} = pvc:read_keys(CommentsToUser, TxId),
    Commit = pvc:commit_transaction(TxId),

    case Commit of
        ?committed ->
            {ok, {Username, [ItemInfo, BoughtInfo, BidInfo, CommentInfo]}};
        {error, Reason} ->
            {error, Reason}
    end.
