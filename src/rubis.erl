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

%% Init DB
-export([put_region/1,
         put_category/1]).

%% RUBIS Procedures
-export([auth_user/2,
         register_user/3,
         browse_categories/0,
         search_items_by_category/1,
         browse_regions/0,
         search_items_by_region/2,
         view_item/1,
         view_user/1,
         view_item_bid_hist/1,
         store_buy_now/3,
         store_bid/3,
         store_comment/5,
         store_item/5,
         about_me/1]).

-spec put_region(binary()) -> {ok, key()}.
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
    {ok, []} = pvc:commit_transaction(TxId),

    {ok, RegionKey}.

-spec put_category(binary()) -> {ok, key()}.
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
    {ok, []} = pvc:commit_transaction(TxId),

    {ok, CategoryKey}.

-spec auth_user(binary(), binary()) -> {ok, key()} | {error, reason()}.
auth_user(Username, Password) ->
    NameIndex = ?ru:gen_index_name(?default_group, users_name),
    {ok, TxId} = pvc:start_transaction(),
    {ok, [Val]} = pvc_indices:read_u_index(NameIndex, Username, TxId),
    Status = case Val of
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
    {ok, []} = pvc:commit_transaction(TxId),

    Status.

-spec register_user(binary(), binary(), binary()) -> {ok, key()} | {error, reason()}.
register_user(Username, Password, RegionName) ->
    %% Users are grouped into their region name group
    RegionPartition = log_utilities:get_key_partition(RegionName),
    SelfGrouping = RegionName,

    %% The user_name index live globally to ensure global uniqueness
    UsernameIndex = ?ru:gen_index_name(?default_group, users_name),
    %% The user_region index should live along the user itself
    RegionIdIndex = ?ru:gen_index_name(SelfGrouping, users_region),

    %% Use the regions_name index to look up the region id
    %% The regions_name index lives globally
    RegionNameIndex = ?ru:gen_index_name(?default_group, regions_name),

    {ok, TxId} = pvc:start_transaction(),

    %% Check that our username is unique
    {ok, [Val]} = pvc_indices:read_u_index(UsernameIndex, Username, TxId),
    Result = case Val of
        <<>> ->
            NextUserId = rubis_keygen_vnode:next_id(RegionPartition, users),
            UserKey = ?ru:gen_key(SelfGrouping, users, NextUserId),

            {ok, [RegionId]} = pvc_indices:read_index(RegionNameIndex, RegionName, TxId),
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
    {ok, []} = pvc:commit_transaction(TxId),

    Result.

-spec browse_categories() -> {ok, list()}.
browse_categories() ->
    CategoryNameIndex = rubis_utils:gen_index_name(?default_group, categories_name),
    {ok, TxId} = pvc:start_transaction(),
    {ok, CategoryKeys} = pvc_indices:read_index(CategoryNameIndex, TxId),
    Result = pvc:read_keys(CategoryKeys, TxId),
    {ok, []} = pvc:commit_transaction(TxId),
    Result.

-spec search_items_by_category(key()) -> {ok, list()}.
search_items_by_category(CategoryId) ->
    CategoryGrouping = ?ru:get_grouping(CategoryId),
    CategoryIndex = ?ru:gen_index_name(CategoryGrouping, items_category_id),
    {ok, TxId} = pvc:start_transaction(),
    {ok, ItemKeys} = pvc_indices:read_index(CategoryIndex, CategoryId, TxId),
    Res = pvc:read_keys(ItemKeys, TxId),
    {ok, []} = pvc:commit_transaction(TxId),
    Res.

-spec browse_regions() -> {ok, list()}.
browse_regions() ->
    RegionNameIndex = rubis_utils:gen_index_name(?default_group, regions_name),
    {ok, TxId} = pvc:start_transaction(),
    {ok, RegionKeys} = pvc_indices:read_index(RegionNameIndex, TxId),
    Result = pvc:read_keys(RegionKeys, TxId),
    {ok, []} = pvc:commit_transaction(TxId),
    Result.

-spec search_items_by_region(key(), key()) -> {ok, list()}.
search_items_by_region(CategoryId, RegionId) ->
    %% Get all items in category CategoryId, such that their sellers
    %% are part of the given region
    {ok, TxId} = pvc:start_transaction(),
    {ok, [RegionName]} = pvc:read_keys(RegionId, TxId),
    UserRegionIndex = rubis_utils:gen_index_name(RegionName, users_region),
    {ok, UsersInRegion} = pvc_indices:read_index(UserRegionIndex, TxId),
    MatchingItems = lists:flatmap(fun(UserKey) ->
        SellerGroup = rubis_utils:get_grouping(UserKey),
        SellerIndex = rubis_utils:gen_index_name(SellerGroup, items_seller_id),
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
    {ok, []} = pvc:commit_transaction(TxId),
    MatchingItems.

-spec view_item(key()) -> {ok, #view_item{}}.
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
    {ok, ViewItem}.

-spec view_user(key()) -> {ok, {binary(), list()}}.
view_user(UserId) ->
    UserGroup = rubis_utils:get_grouping(UserId),
    CommentIndex = rubis_utils:gen_index_name(UserGroup, comments_to_id),

    {ok, TxId} = pvc:start_transaction(),
    {ok, [#user{username = Username}]} = pvc:read_keys(UserId, TxId),
    {ok, CommentsToUser} = pvc_indices:read_index(CommentIndex, UserId, TxId),
    CommentInfo = lists:map(fun(CommentId) ->
        {ok, [Comment = #comment{from = FromUserId}]} = pvc:read_keys(CommentId, TxId),
        {ok, [#user{username = FromUsername}]} = pvc:read_keys(FromUserId, TxId),
        {Comment, FromUsername}
                            end, CommentsToUser),
    {ok, []} = pvc:commit_transaction(TxId),
    {ok, {Username, CommentInfo}}.

-spec view_item_bid_hist(key()) -> {ok, {binary(), list()}}.
view_item_bid_hist(ItemId) ->
    SelfGrouping = rubis_utils:get_grouping(ItemId),
    OnItemIdIndex = ?ru:gen_index_name(SelfGrouping, bids_on_item_id),

    {ok, TxId} = pvc:start_transaction(),
    {ok, [#item{name = ItemName}]} = pvc:read_keys(ItemId, TxId),
    {ok, BidsOnItem} = pvc_indices:read_index(OnItemIdIndex, ItemId, TxId),
    BidInfo = lists:map(fun(BidId) ->
        {ok, [Bid = #bid{bidder = UserId}]} = pvc:read_keys(BidId, TxId),
        {ok, [#user{username = Username}]} = pvc:read_keys(UserId, TxId),
        {Bid, Username}
                        end, BidsOnItem),
    {ok, []} = pvc:commit_transaction(TxId),
    {ok, {ItemName, BidInfo}}.

-spec store_buy_now(key(), key(), non_neg_integer()) -> {ok, key()}.
store_buy_now(OnItemId, BuyerId, Quantity) ->
    %% The grouping for a buy_now object is the items's grouping (OnItemid)
    SelfPartition = log_utilities:get_key_partition(OnItemId),
    SelfGrouping = ?ru:get_grouping(OnItemId),

    %% The grouping for buy_now_buyer_id is the Buyer's group (BuyerId)
    BuyerGrouping = rubis_utils:get_grouping(BuyerId),
    BuyerIndex = rubis_utils:gen_index_name(BuyerGrouping, buy_now_buyer_id),

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
    {ok, []} = pvc:commit_transaction(TxId),

    {ok, BuyNowKey}.

-spec store_bid(key(), key(), non_neg_integer()) -> {ok, key()}.
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
    {ok, []} = pvc:commit_transaction(TxId),

    {ok, BidKey}.

-spec store_comment(key(), key(), key(), integer(), binary()) -> {ok, key()}.
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
    {ok, []} = pvc:commit_transaction(TxId),

    {ok, CommentKey}.

-spec store_item(binary(), binary(), non_neg_integer(), key(), key()) -> {ok, key()}.
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
    {ok, []} = pvc:commit_transaction(TxId),

    {ok, ItemKey}.

about_me(UserId) ->
    UserGroup = rubis_utils:get_grouping(UserId),
    SellerIndex = ?ru:gen_index_name(UserGroup, items_seller_id),
    BidderIndex = ?ru:gen_index_name(UserGroup, bids_bidder_id),
    CommentIndex = rubis_utils:gen_index_name(UserGroup, comments_to_id),
    BuyerIndex = rubis_utils:gen_index_name(UserGroup, buy_now_buyer_id),

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
    {ok, []} = pvc:commit_transaction(TxId),
    {ok, {Username, [ItemInfo, BoughtInfo, BidInfo, CommentInfo]}}.
