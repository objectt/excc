/*
 * Description:
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_market.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"
# include "me_trade.h"

uint64_t order_id_start;
uint64_t deals_id_start;

struct dict_user_key {
    uint32_t    user_id;
};

struct dict_order_key {
    uint64_t    order_id;
};

static uint32_t dict_user_hash_function(const void *key)
{
    const struct dict_user_key *obj = key;
    return obj->user_id;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    const struct dict_user_key *obj1 = key1;
    const struct dict_user_key *obj2 = key2;
    if (obj1->user_id == obj2->user_id) {
        return 0;
    }
    return 1;
}

static void *dict_user_key_dup(const void *key)
{
    struct dict_user_key *obj = malloc(sizeof(struct dict_user_key));
    memcpy(obj, key, sizeof(struct dict_user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void dict_user_val_free(void *key)
{
    skiplist_release(key);
}

static uint32_t dict_order_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_order_key));
}

static int dict_order_key_compare(const void *key1, const void *key2)
{
    const struct dict_order_key *obj1 = key1;
    const struct dict_order_key *obj2 = key2;
    if (obj1->order_id == obj2->order_id) {
        return 0;
    }
    return 1;
}

static void *dict_order_key_dup(const void *key)
{
    struct dict_order_key *obj = malloc(sizeof(struct dict_order_key));
    memcpy(obj, key, sizeof(struct dict_order_key));
    return obj;
}

static void dict_order_key_free(void *key)
{
    free(key);
}

static int order_match_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }
    if (order1->type != order2->type) {
        return 1;
    }

    int cmp;
    if (order1->side == MARKET_ORDER_SIDE_ASK) {
        cmp = mpd_cmp(order1->price, order2->price, &mpd_ctx);
    } else {
        cmp = mpd_cmp(order2->price, order1->price, &mpd_ctx);
    }
    if (cmp != 0) {
        return cmp;
    }

    return order1->id > order2->id ? 1 : -1;
}

static int order_id_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;
    if (order1->id == order2->id) {
        return 0;
    }

    return order1->id > order2->id ? -1 : 1;
}

static void order_free(order_t *order)
{
    mpd_del(order->price);
    mpd_del(order->amount);
    mpd_del(order->taker_fee);
    mpd_del(order->maker_fee);
    mpd_del(order->left);
    mpd_del(order->freeze);
    mpd_del(order->deal_stock);
    mpd_del(order->deal_money);
    mpd_del(order->deal_fee);
    free(order->market);
    free(order->source);
    free(order);
}

json_t *get_order_info(order_t *order)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "market", json_string(order->market));
    json_object_set_new(info, "source", json_string(order->source));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "side", json_integer(order->side));
    json_object_set_new(info, "user", json_integer(order->user_id));
    json_object_set_new(info, "ctime", json_real(order->create_time));
    json_object_set_new(info, "mtime", json_real(order->update_time));

    json_object_set_new_mpd(info, "price", order->price);
    json_object_set_new_mpd(info, "amount", order->amount);
    json_object_set_new_mpd(info, "taker_fee", order->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", order->maker_fee);
    json_object_set_new_mpd(info, "left", order->left);
    json_object_set_new_mpd(info, "deal_stock", order->deal_stock);
    json_object_set_new_mpd(info, "deal_money", order->deal_money);
    json_object_set_new_mpd(info, "deal_fee", order->deal_fee);

    return info;
}

static int order_put(market_t *m, order_t *order)
{
    if (order->type != MARKET_ORDER_TYPE_LIMIT && order->type != MARKET_ORDER_TYPE_AON)
        return -__LINE__;

    order->role = MARKET_ROLE_MAKER;

    struct dict_order_key order_key = { .order_id = order->id };
    if (dict_add(m->orders, &order_key, order) == NULL)
        return -__LINE__;

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(m->users, &user_key, order_list) == NULL)
            return -__LINE__;
    }

    if (order->side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_insert(m->asks, order) == NULL)
            return -__LINE__;
        mpd_copy(order->freeze, order->left, &mpd_ctx);
        if (balance_freeze(order->user_id, m->stock, order->left) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->bids, order) == NULL)
            return -__LINE__;
        mpd_t *result = mpd_new(&mpd_ctx);
        mpd_t *max_fee = mpd_new(&mpd_ctx);

        mpd_mul(result, order->price, order->left, &mpd_ctx);
        mpd_mul(max_fee, result, order->taker_fee, &mpd_ctx);
        mpd_add(result, result, max_fee, &mpd_ctx);

        mpd_copy(order->freeze, result, &mpd_ctx);
        if (balance_freeze(order->user_id, m->money, result) == NULL) {
            mpd_del(result);
            return -__LINE__;
        }

        mpd_del(result);
        mpd_del(max_fee);
    }

    return 0;
}

static int order_finish(bool real, market_t *m, order_t *order)
{
    if (order->side == MARKET_ORDER_SIDE_ASK) {
        skiplist_node *node = skiplist_find(m->asks, order);
        if (node) {
            skiplist_delete(m->asks, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, m->stock, order->freeze) == NULL) {
                return -__LINE__;
            }
        }
    } else {
        skiplist_node *node = skiplist_find(m->bids, order);
        if (node) {
            skiplist_delete(m->bids, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, m->money, order->freeze) == NULL) {
                return -__LINE__;
            }
        }
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->orders, &order_key);

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }

        mpd_t *balance = balance_total(order->user_id, m->name);
        if (mpd_cmp(balance, mpd_zero, &mpd_ctx) == 0) {
            // Remove from dict users if balance is zero
            dict_delete(m->users, &user_key);
        }
    }

    if (real) {
        if (mpd_cmp(order->deal_stock, mpd_zero, &mpd_ctx) > 0) {
            int ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
        }
    }

    order_free(order);
    return 0;
}

market_t *market_create(struct market *conf)
{
    if (!asset_exist(conf->stock) || !asset_exist(conf->money))
        return NULL;
    if (conf->stock_prec + conf->money_prec > asset_prec(conf->money))
        return NULL;
    if (conf->stock_prec + conf->fee_prec > asset_prec(conf->stock))
        return NULL;
    if (conf->money_prec + conf->fee_prec > asset_prec(conf->money))
        return NULL;

    market_t *m = malloc(sizeof(market_t));
    memset(m, 0, sizeof(market_t));
    m->name             = strdup(conf->name);
    m->stock            = strdup(conf->stock);
    m->money            = strdup(conf->money);
    m->fee              = strdup(conf->fee);
    m->include_fee      = true;
    m->stock_prec       = conf->stock_prec;
    m->money_prec       = conf->money_prec;
    m->fee_prec         = conf->fee_prec;
    m->delisting_ts     = conf->delisting_ts;
    m->min_amount       = mpd_qncopy(conf->min_amount);
    //m->max_amount       = mpd_qncopy(conf->max_amount);
    m->min_price        = mpd_qncopy(conf->min_price);
    //m->max_price        = mpd_qncopy(conf->max_price);
    m->min_total        = mpd_qncopy(conf->min_total);
    m->last_price       = mpd_qncopy(conf->closing_price);
    m->closing_price    = mpd_qncopy(conf->closing_price);

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_user_hash_function;
    dt.key_compare      = dict_user_key_compare;
    dt.key_dup          = dict_user_key_dup;
    dt.key_destructor   = dict_user_key_free;
    dt.val_destructor   = dict_user_val_free;

    m->users = dict_create(&dt, 1024);
    if (m->users == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_order_hash_function;
    dt.key_compare      = dict_order_key_compare;
    dt.key_dup          = dict_order_key_dup;
    dt.key_destructor   = dict_order_key_free;

    m->orders = dict_create(&dt, 1024);
    if (m->orders == NULL)
        return NULL;

    skiplist_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare          = order_match_compare;

    m->asks = skiplist_create(&lt);
    m->bids = skiplist_create(&lt);
    if (m->asks == NULL || m->bids == NULL)
        return NULL;

    return m;
}

static int append_balance_trade_add(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", change, detail_str);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int append_balance_trade_sub(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}


static int append_balance_trade_fee(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount, mpd_t *fee_rate)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    json_object_set_new_mpd(detail, "f", fee_rate);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

bool check_price_limit(mpd_t *cmp_price, mpd_t *price, const char *pct)
{
    if (cmp_price == NULL)
        return false;

    if (mpd_cmp(price, mpd_zero, &mpd_ctx) == 0
        || mpd_cmp(cmp_price, mpd_zero, &mpd_ctx) == 0
        || mpd_cmp(price, cmp_price, &mpd_ctx) == 0)
        return true;

    mpd_t *limit = decimal(pct, 2);
    if (mpd_cmp(limit, mpd_zero, &mpd_ctx) == 0) {
        mpd_del(limit);
        return true;
    }

    mpd_t *range = mpd_new(&mpd_ctx);
    mpd_t *diff = mpd_new(&mpd_ctx);
    mpd_mul(range, cmp_price, limit, &mpd_ctx);
    mpd_del(limit);

    mpd_sub(diff, price, cmp_price, &mpd_ctx);
    mpd_abs(diff, diff, &mpd_ctx);

    // Within N% of last price
    if (mpd_cmp(diff, range, &mpd_ctx) > 0) {
        mpd_del(range);
        mpd_del(diff);
        return false;
    }

    mpd_del(range);
    mpd_del(diff);

    return true;
}

static void process_ask_order(bool real, market_t *m, mpd_t *amount, mpd_t *price, mpd_t *deal,
                                 order_t *seller, mpd_t *ask_fee, mpd_t *fee)
{
    mpd_sub(seller->left, seller->left, amount, &mpd_ctx);
    mpd_add(seller->deal_stock, seller->deal_stock, amount, &mpd_ctx);
    mpd_add(seller->deal_money, seller->deal_money, deal, &mpd_ctx);
    mpd_add(seller->deal_fee, seller->deal_fee, ask_fee, &mpd_ctx);

    // Seller withdrawl
    if (seller->role == MARKET_ROLE_MAKER) {
        mpd_sub(seller->freeze, seller->freeze, amount, &mpd_ctx);
        balance_sub(seller->user_id, BALANCE_TYPE_FREEZE, m->stock, amount);
    }
    else
        balance_sub(seller->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);

    if (real)
        append_balance_trade_sub(seller, m->stock, amount, price, amount);

    // Seller deposit
    balance_add(seller->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
    if (real)
        append_balance_trade_add(seller, m->money, deal, price, amount);

    // Ask fee
    if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
        balance_sub(seller->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
        if (real)
            append_balance_trade_fee(seller, m->money, ask_fee, price, amount, fee);
    }
}

static void process_bid_order(bool real, market_t *m, mpd_t *amount, mpd_t *price, mpd_t *deal,
                                 order_t *bidder, mpd_t *bid_fee, mpd_t *fee)
{
    mpd_sub(bidder->left, bidder->left, amount, &mpd_ctx);
    mpd_add(bidder->deal_stock, bidder->deal_stock, amount, &mpd_ctx);
    mpd_add(bidder->deal_money, bidder->deal_money, deal, &mpd_ctx);
    mpd_add(bidder->deal_fee, bidder->deal_fee, bid_fee, &mpd_ctx);

    // Bidder withdrawl
    if (bidder->role == MARKET_ROLE_MAKER) {
        mpd_sub(bidder->freeze, bidder->freeze, deal, &mpd_ctx);
        balance_sub(bidder->user_id, BALANCE_TYPE_FREEZE, m->money, deal);
    }
    else
        balance_sub(bidder->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);

    if (real)
        append_balance_trade_sub(bidder, m->money, deal, price, amount);

    // Bidder deposit
    balance_add(bidder->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
    if (real)
        append_balance_trade_add(bidder, m->stock, amount, price, amount);

    // Bid fee
    if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
        if (bidder->role == MARKET_ROLE_MAKER && m->include_fee) {
            mpd_sub(bidder->freeze, bidder->freeze, bid_fee, &mpd_ctx);
            balance_sub(bidder->user_id, BALANCE_TYPE_FREEZE, m->fee, bid_fee);
        }
        else
            balance_sub(bidder->user_id, BALANCE_TYPE_AVAILABLE, m->fee, bid_fee);

        if (real)
            append_balance_trade_fee(bidder, m->fee, bid_fee, price, amount, fee);
    }
}

static void process_order(bool real, market_t *m, mpd_t *amount, mpd_t *price, mpd_t *deal,
    order_t *seller, order_t *bidder, mpd_t *ask_fee, mpd_t * bid_fee, uint32_t side)
{
    seller->update_time = bidder->update_time = current_timestamp();
    uint64_t deal_id = ++deals_id_start;
    if (real) {
        append_order_deal_history(seller->update_time, deal_id, seller, seller->role,
            bidder, bidder->role, price, amount, deal, ask_fee, bid_fee);
        push_deal_message(seller->update_time, m->name, seller, bidder, price, amount,
            ask_fee, bid_fee, side, deal_id, m->stock, m->money);
    }

    order_t *maker = (seller->role == MARKET_ROLE_MAKER)? seller : bidder;
    if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
        if (real)
            push_order_message(ORDER_EVENT_FINISH, maker, m, amount);
        order_finish(real, m, maker);
    }
    else if (real)
        push_order_message(ORDER_EVENT_UPDATE, maker, m, amount);

    // Last price
    mpd_copy(m->last_price, price, &mpd_ctx);
}

static void post_process_order(market_t *m, order_t *order, bool is_maker_candidate, bool real)
{
    int ret = 0;
    mpd_t *filled = mpd_new(&mpd_ctx);
    mpd_sub(filled, order->amount, order->left, &mpd_ctx);

    if (!is_maker_candidate ||
        mpd_cmp(order->left, mpd_zero, &mpd_ctx) == 0) {
        add_user_to_market(m->name, order->user_id);

        if (real) {
            ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
            push_order_message(ORDER_EVENT_FINISH, order, m, filled);
        }
        order_free(order);
    }
    else {
        if (real) {
            push_order_message(ORDER_EVENT_PUT, order, m, filled);
        }
        ret = order_put(m, order);
        if (ret < 0) {
            log_fatal("order_put fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    mpd_del(filled);
}

static int execute_limit_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0)
            break;

        order_t *maker = node->value;

        // Price
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) > 0)
            break;
        // Amount : Limit to AON
        if (maker->type == MARKET_ORDER_TYPE_AON &&
            mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0)
            continue;
        // Amount
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0)
            mpd_copy(amount, taker->left, &mpd_ctx);
        else
            mpd_copy(amount, maker->left, &mpd_ctx);

        mpd_copy(price, maker->price, &mpd_ctx);
        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);

        if (!m->include_fee)
            mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);
        else
            mpd_mul(bid_fee, deal, maker->maker_fee, &mpd_ctx);

        process_ask_order(real, m, amount, price, deal, taker, ask_fee, taker->taker_fee);
        process_bid_order(real, m, amount, price, deal, maker, bid_fee, maker->maker_fee);
        process_order(real, m, amount, price, deal, taker, maker, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);

    return 0;
}

static int execute_limit_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0)
            break;

        order_t *maker = node->value;

        // Price
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) < 0)
            break;
        // Amount : Limit to AON
        if (maker->type == MARKET_ORDER_TYPE_AON &&
            mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0)
            continue;
        // Amount
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0)
            mpd_copy(amount, taker->left, &mpd_ctx);
        else
            mpd_copy(amount, maker->left, &mpd_ctx);

        mpd_copy(price, maker->price, &mpd_ctx);
        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);

        if (!m->include_fee)
            mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);
        else
            mpd_mul(bid_fee, deal, taker->taker_fee, &mpd_ctx);

        process_ask_order(real, m, amount, price, deal, maker, ask_fee, maker->maker_fee);
        process_bid_order(real, m, amount, price, deal, taker, bid_fee, taker->taker_fee);
        process_order(real, m, amount, price, deal, maker, taker, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);

    return 0;
}

static int execute_all_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal    = mpd_new(&mpd_ctx);
    mpd_t *ask_fee = mpd_new(&mpd_ctx);
    mpd_t *bid_fee = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0)
            break;

        order_t *maker = node->value;

        // Price
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) > 0)
            break;
        // Amount - All or nothing
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) > 0)
            continue;
        // Amount : AON to AON
        if (maker->type == MARKET_ORDER_TYPE_AON &&
            mpd_cmp(taker->left, maker->left, &mpd_ctx) != 0)
            continue;

        mpd_copy(price, maker->price, &mpd_ctx);
        mpd_copy(amount, taker->left, &mpd_ctx); // All or Nothing
        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);

        if (!m->include_fee)
            mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);
        else
            mpd_mul(bid_fee, deal, maker->maker_fee, &mpd_ctx);

        process_ask_order(real, m, amount, price, deal, taker, ask_fee, taker->taker_fee);
        process_bid_order(real, m, amount, price, deal, maker, bid_fee, maker->maker_fee);
        process_order(real, m, amount, price, deal, taker, maker, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(deal);

    return 0;
}

static int execute_all_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0)
            break;

        order_t *maker = node->value;

        // Price
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) < 0)
            break;
        // Amount
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) > 0)
            continue;
        // Amount : FOK/AON to AON
        if (maker->type == MARKET_ORDER_TYPE_AON &&
            mpd_cmp(taker->left, maker->left, &mpd_ctx) != 0)
            continue;

        mpd_copy(amount, taker->left, &mpd_ctx); // All or Nothing
        mpd_copy(price, maker->price, &mpd_ctx);
        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);

        if (!m->include_fee)
            mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);
        else
            mpd_mul(bid_fee, deal, taker->taker_fee, &mpd_ctx);

        process_ask_order(real, m, amount, price, deal, maker, ask_fee, maker->maker_fee);
        process_bid_order(real, m, amount, price, deal, taker, bid_fee, taker->taker_fee);
        process_order(real, m, amount, price, deal, maker, taker, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);

    return 0;
}

int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price, mpd_t *taker_fee, mpd_t *maker_fee, const char *source)
{
    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->role         = MARKET_ROLE_TAKER;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, maker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_limit_ask_order(real, m, order);
    } else {
        ret = execute_limit_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    post_process_order(m, order, true, real);

    if (real)
        *result = get_order_info(order);

    return 0;
}

static int execute_market_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price   = mpd_new(&mpd_ctx);
    mpd_t *amount  = mpd_new(&mpd_ctx);
    mpd_t *deal    = mpd_new(&mpd_ctx);
    mpd_t *ask_fee = mpd_new(&mpd_ctx);
    mpd_t *bid_fee = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0)
            break;

        order_t *maker = node->value;

        // Amount : Limit to AON
        if (maker->type == MARKET_ORDER_TYPE_AON &&
            mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0)
            continue;
        // Amount
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0)
            mpd_copy(amount, taker->left, &mpd_ctx);
        else
            mpd_copy(amount, maker->left, &mpd_ctx);

        mpd_copy(price, maker->price, &mpd_ctx);
        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);

        if (!m->include_fee)
            mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);
        else
            mpd_mul(bid_fee, deal, maker->maker_fee, &mpd_ctx);

        process_ask_order(real, m, amount, price, deal, taker, ask_fee, taker->taker_fee);
        process_bid_order(real, m, amount, price, deal, maker, bid_fee, maker->maker_fee);
        process_order(real, m, amount, price, deal, taker, maker, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK);
    }
    skiplist_release_iterator(iter);

    mpd_del(price);
    mpd_del(amount);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);

    return 0;
}

static int execute_market_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0)
            break;

        order_t *maker = node->value;

        // Amount : Limit to AON
        if (maker->type == MARKET_ORDER_TYPE_AON &&
            mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0)
            continue;

        mpd_copy(price, maker->price, &mpd_ctx);

        // Round off
        mpd_div(amount, taker->left, price, &mpd_ctx);
        mpd_rescale(amount, amount, -m->stock_prec, &mpd_ctx);
        while (true) {
            mpd_mul(result, amount, price, &mpd_ctx);
            if (mpd_cmp(result, taker->left, &mpd_ctx) > 0) {
                mpd_set_i32(result, -m->stock_prec, &mpd_ctx);
                mpd_pow(result, mpd_ten, result, &mpd_ctx);
                mpd_sub(amount, amount, result, &mpd_ctx); // subtract smallest unit
            } else {
                break;
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            log_error("empty maker order found: %"PRIu64"", maker->id);
            continue;
        }
        if (mpd_cmp(amount, maker->left, &mpd_ctx) > 0)
            mpd_copy(amount, maker->left, &mpd_ctx);

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);

        if (!m->include_fee)
            mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);
        else
            mpd_mul(bid_fee, deal, taker->taker_fee, &mpd_ctx);

        process_ask_order(real, m, amount, price, deal, maker, ask_fee, maker->maker_fee);
        process_bid_order(real, m, amount, price, deal, taker, bid_fee, taker->taker_fee);
        process_order(real, m, amount, price, deal, maker, taker, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID);
    }
    skiplist_release_iterator(iter);

    mpd_del(price);
    mpd_del(amount);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *taker_fee, const char *source)
{
    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->role         = MARKET_ROLE_TAKER;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
    } else {
        ret = execute_market_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    post_process_order(m, order, false, real);

    if (real)
        *result = get_order_info(order);

    return 0;
}

int market_put_fok_order(bool real, json_t **result, market_t *m,
    uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price,
    mpd_t *taker_fee, const char *source)
{
    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->role         = MARKET_ROLE_TAKER;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_all_ask_order(real, m, order);
    } else {
        ret = execute_all_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute fok order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    post_process_order(m, order, false, real);

    if (real)
        *result = get_order_info(order);

    return 0;
}

int market_put_aon_order(bool real, json_t **result, market_t *m,
    uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price,
    mpd_t *taker_fee, mpd_t *maker_fee, const char *source)
{
    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_AON;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_all_ask_order(real, m, order);
    } else {
        ret = execute_all_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute fok order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    post_process_order(m, order, true, real);

    if (real)
        *result = get_order_info(order);

    return 0;
}

int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)
{
    if (real) {
        push_order_message(ORDER_EVENT_FINISH, order, m, mpd_zero);
        *result = get_order_info(order);
    }
    order_finish(real, m, order);
    return 0;
}

int market_put_order(market_t *m, order_t *order)
{
    return order_put(m, order);
}

order_t *market_get_order(market_t *m, uint64_t order_id)
{
    struct dict_order_key key = { .order_id = order_id };
    dict_entry *entry = dict_find(m->orders, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

skiplist_t *market_get_order_list(market_t *m, uint32_t user_id)
{
    struct dict_user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(m->users, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

int market_get_status(market_t *m, size_t *ask_count, mpd_t *ask_amount, size_t *bid_count, mpd_t *bid_amount)
{
    *ask_count = m->asks->len;
    *bid_count = m->bids->len;
    mpd_copy(ask_amount, mpd_zero, &mpd_ctx);
    mpd_copy(bid_amount, mpd_zero, &mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(ask_amount, ask_amount, order->left, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(bid_amount, bid_amount, order->left, &mpd_ctx);
    }

    return 0;
}

sds market_status(sds reply)
{
    reply = sdscatprintf(reply, "order last ID: %"PRIu64"\n", order_id_start);
    reply = sdscatprintf(reply, "deals last ID: %"PRIu64"\n", deals_id_start);
    return reply;
}

int asset_register(const char *symbol, const char *name, const char *tick_size)
{
    log_debug("registering a new asset - %s", symbol);

    sds sql = sdsnew("INSERT INTO assets (symbol, name, tick_size) VALUES (");
    sql = sdscatprintf(sql, "UPPER('%s'),", symbol);
    sql = sdscatprintf(sql, "'%s',", name);
    sql = sdscatprintf(sql, "'%s')", tick_size);

    MYSQL *conn = mysql_connect(&settings.db_sys);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        mysql_close(conn);
        return -__LINE__;
    }
    int asset_id = mysql_insert_id(conn);
    sdsfree(sql);
    mysql_close(conn);

    return asset_id;
}

int market_register(const char *symbol, const char *name, int base_id, int counter_id,
                    const char *init_price, uint32_t delisting_ts)
{
    log_debug("registering a new market - %s @ %s", symbol, init_price);

    sds sql = sdsnew("INSERT INTO market "
                     "(symbol, name, stock, currency, delisting_ts, init_price, closing_price) VALUES (");
    sql = sdscatprintf(sql, "UPPER('%s'),", symbol);
    sql = sdscatprintf(sql, "'%s',", name);
    sql = sdscatprintf(sql, "%u,", base_id);
    sql = sdscatprintf(sql, "%u,", counter_id);
    sql = sdscatprintf(sql, "%u,", delisting_ts);
    sql = sdscatprintf(sql, "'%s',", init_price);
    sql = sdscatprintf(sql, "'%s')", init_price);

    MYSQL *conn = mysql_connect(&settings.db_sys);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        mysql_close(conn);
        return -__LINE__;
    }
    int market_id = mysql_insert_id(conn);
    sdsfree(sql);
    mysql_close(conn);

    return market_id;
}

json_t *market_detail(market_t *market)
{
    json_t *result = json_array();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(market->users);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_user_key *key = entry->key;

        mpd_t *total = mpd_new(&mpd_ctx);
        mpd_copy(total, mpd_zero, &mpd_ctx);
        mpd_t *available = balance_get(key->user_id, BALANCE_TYPE_AVAILABLE, market->name);
        mpd_t *freeze = balance_get(key->user_id, BALANCE_TYPE_FREEZE, market->name);

        json_t *row = json_object();
        json_object_set_new(row, "user_id", json_integer(key->user_id));

        if (!available) {
            json_object_set_new(row, "available", json_string("0"));
        } else {
            json_object_set_new_mpd(row, "available", available);
            mpd_add(total, total, available, &mpd_ctx);
        }

        if (!freeze) {
            json_object_set_new(row, "freeze", json_string("0"));
        } else {
            json_object_set_new_mpd(row, "freeze", freeze);
            mpd_add(total, total, freeze, &mpd_ctx);
        }

        json_object_set_new_mpd(row, "total", total);

        if (mpd_cmp(total, mpd_zero, &mpd_ctx) > 0)
            json_array_append_new(result, row);

        mpd_del(total);
    }
    dict_release_iterator(iter);

    return result;
}

int add_user_to_market(const char *market, uint32_t user_id)
{
    market_t *m = get_market(market);
    if (!m)
        return 0;

    struct dict_user_key user_key = { .user_id = user_id };

    dict_entry *entry = dict_find(m->users, &user_key);
    if (!entry) {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);

        if (dict_add(m->users, &user_key, order_list) == NULL)
            return -__LINE__;
    }

    return 0;
}
