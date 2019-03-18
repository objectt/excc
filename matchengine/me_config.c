/*
 * Description:
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_market.h"
# include "me_balance.h"
# include "me_trade.h"

struct settings settings;

static nw_job *job;
static nw_periodic daily_periodic;
static nw_periodic min_periodic;

struct job_request {
    void (*callback_fn)(MYSQL*);
};

// Load assets config from database
static int load_assets_from_db(MYSQL *conn)
{
    sds sql = sdsnew("SELECT id, name, prec_save, prec_show, min_amount "
                     "FROM assets WHERE id = 1 OR is_listed = 1");
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -1;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    settings.asset_num = mysql_num_rows(result);
    settings.assets = malloc(sizeof(struct asset) * settings.asset_num);
    for (size_t i = 0; i < settings.asset_num; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);

        settings.assets[i].id = strtoul(row[0], NULL, 0);
        settings.assets[i].name = strdup(row[1]);
        settings.assets[i].prec_save = strtoul(row[2], NULL, 0);
        settings.assets[i].prec_show = strtoul(row[3], NULL, 0);
        settings.assets[i].min_amount = decimal(row[4], settings.assets[i].prec_save);

        log_debug("asset loaded - %s, %s",
                  settings.assets[i].name, mpd_to_sci(settings.assets[i].min_amount, 0));
    }
    mysql_free_result(result);

    return 0;
}

// Load newly added assets
static int update_assets_from_db(MYSQL *conn)
{
    sds sql = sdsnew("SELECT id, name, prec_save, prec_show, min_amount FROM assets A "
                     "WHERE A.is_listed = 0 AND A.id != 1");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -1;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    int asset_num = mysql_num_rows(result);
    if (asset_num == 0) {
        mysql_free_result(result);
        return asset_num;
    }

    asset_info_t *new_asset = realloc(settings.assets, sizeof(asset_info_t) * (settings.asset_num + asset_num));
    if (new_asset == NULL) {
        mysql_free_result(result);
        return -__LINE__;
    }

    int asset_cnt = settings.asset_num;
    settings.assets = new_asset;

    log_debug("%u new assets found", asset_num);
    for (size_t i = settings.asset_num; i < settings.asset_num + asset_num; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);

        if (asset_exist(strdup(row[1])))
            continue;

        settings.assets[i].id = strtoul(row[0], NULL, 0);
        settings.assets[i].name = strdup(row[1]);
        settings.assets[i].prec_save = strtoul(row[2], NULL, 0);
        settings.assets[i].prec_show = strtoul(row[3], NULL, 0);
        settings.assets[i].min_amount = decimal(row[4], settings.assets[i].prec_save);

        update_asset(&(settings.assets[i])); // update dict_asset
        asset_cnt++;

        log_debug("new asset loaded - %s, %s",
                  settings.assets[i].name, mpd_to_sci(settings.assets[i].min_amount, 0));
    }
    mysql_free_result(result);

    settings.asset_num = asset_cnt;

    return asset_num;
}

// Load market config from database
static int load_market_from_db(MYSQL *conn)
{
    sds sql = sdsnew("SELECT A1.name, A1.prec_show AS stock_prec, M.min_amount, "
                     "A2.name AS currency_name, A2.prec_show AS currency_prec, "
                     "M.fee_prec, M.init_price, M.closing_price, M.id "
                     "FROM market M, assets A1, assets A2 "
                     "WHERE M.stock = A1.id AND M.currency = A2.id AND A1.is_listed = 1;");
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -1;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    settings.market_num = mysql_num_rows(result);
    settings.markets = malloc(sizeof(struct market) * settings.market_num);
    for (size_t i = 0; i < settings.market_num; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);

        settings.markets[i].id = strtoul(row[8], NULL, 0);
        settings.markets[i].name = strdup(row[0]);
        settings.markets[i].fee_prec = strtoul(row[5], NULL, 0);
        settings.markets[i].min_amount = decimal(row[2], 8);

        settings.markets[i].stock = strdup(row[0]);
        settings.markets[i].stock_prec = strtoul(row[1], NULL, 0);
        settings.markets[i].money = strdup(row[3]);
        settings.markets[i].money_prec = strtoul(row[4], NULL, 0);
        settings.markets[i].init_price = decimal(row[6], 8);
        settings.markets[i].closing_price = decimal(row[7], 8);
    }
    mysql_free_result(result);

    return 0;
}

static int update_asset_status(MYSQL *conn, int asset_id)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "UPDATE assets SET is_listed = 1 WHERE id = %d", asset_id);

    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -1;
    }
    sdsfree(sql);

    return 1;
}

// Update market list
static int update_market_from_db(MYSQL *conn)
{
    sds sql = sdsnew("SELECT A1.name, A1.prec_show AS stock_prec, M.min_amount, "
                     "A2.name AS currency_name, A2.prec_show AS currency_prec, "
                     "M.fee_prec, M.init_price, M.closing_price, M.id, M.stock "
                     "FROM market M, assets A1, assets A2 "
                     "WHERE M.stock = A1.id AND M.currency = A2.id AND A1.is_listed = 0;");
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -1;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    int market_num = mysql_num_rows(result);
    if (market_num == 0) {
        mysql_free_result(result);
        return 0;
    }

    market_info_t *new_market = realloc(settings.markets,
                                        sizeof(market_info_t) * (settings.market_num + market_num));
    if (new_market == NULL) {
        mysql_free_result(result);
        return -__LINE__;
    }

    int market_cnt = settings.market_num;
    settings.markets = new_market;

    log_debug("%u new stocks found", market_num);
    for (size_t i = settings.market_num; i < settings.market_num + market_num; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);

        if (get_market(strdup(row[0])))
            continue;

        settings.markets[i].id = strtoul(row[8], NULL, 0);
        settings.markets[i].name = strdup(row[0]);
        settings.markets[i].fee_prec = strtoul(row[5], NULL, 0);
        settings.markets[i].min_amount = decimal(row[2], 0);

        settings.markets[i].stock = strdup(row[0]);
        settings.markets[i].stock_prec = strtoul(row[1], NULL, 0);
        settings.markets[i].money = strdup(row[3]);
        settings.markets[i].money_prec = strtoul(row[4], NULL, 0);
        settings.markets[i].init_price = decimal(row[6], 0);
        settings.markets[i].closing_price = decimal(row[7], 0);

        if (update_market(&(settings.markets[i])) == 0) // update dict_market
            update_asset_status(conn, strtoul(row[9], NULL, 0));

        market_cnt++;

        log_debug("new stock loaded - %s", settings.markets[i].name);
    }
    mysql_free_result(result);

    settings.market_num = market_cnt;

    return market_num;
}

static int read_config_from_json(json_t *root)
{
    int ret;
    ret = read_cfg_bool(root, "debug", &settings.debug, false, false);
    if (ret < 0) {
        printf("read debug config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_process(root, "process", &settings.process);
    if (ret < 0) {
        printf("load process config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_log(root, "log", &settings.log);
    if (ret < 0) {
        printf("load log config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_alert(root, "alert", &settings.alert);
    if (ret < 0) {
        printf("load alert config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_rpc_svr(root, "svr", &settings.svr);
    if (ret < 0) {
        printf("load svr config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_cli_svr(root, "cli", &settings.cli);
    if (ret < 0) {
        printf("load cli config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_mysql(root, "db_sys", &settings.db_sys);
    if (ret < 0) {
        printf("load log db config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_mysql(root, "db_log", &settings.db_log);
    if (ret < 0) {
        printf("load log db config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_mysql(root, "db_history", &settings.db_history);
    if (ret < 0) {
        printf("load history db config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_kafka_producer(root, "kafka", &settings.producer);
    if (ret < 0) {
        printf("load kafka config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = read_cfg_int(root, "slice_interval", &settings.slice_interval, false, 86400);
    if (ret < 0) {
        printf("load slice_interval fail: %d", ret);
        return -__LINE__;
    }
    ret = read_cfg_int(root, "slice_keeptime", &settings.slice_keeptime, false, 86400 * 3);
    if (ret < 0) {
        printf("load slice_keeptime fail: %d", ret);
        return -__LINE__;
    }
    ret = read_cfg_int(root, "history_thread", &settings.history_thread, false, 10);
    if (ret < 0) {
        printf("load history_thread fail: %d", ret);
        return -__LINE__;
    }

    ERR_RET_LN(read_cfg_real(root, "cache_timeout", &settings.cache_timeout, false, 0.45));

    return 0;
}

static void load_new_assets_market(MYSQL *conn)
{
    if (update_assets_from_db(conn) > 0)
        update_market_from_db(conn);
}

static void update_market_closing_price(MYSQL *conn)
{
    for (size_t i = 0; i < settings.market_num; ++i) {
        market_t *m = get_market(settings.markets[i].name);
        sds sql = sdsempty();

        mpd_copy(settings.markets[i].closing_price, m->last_price, &mpd_ctx);
        mpd_copy(m->closing_price, m->last_price, &mpd_ctx);
        char *closing_price = mpd_to_sci(m->closing_price, 0);
        log_debug("set closing price - %s = %s", m->name, closing_price);

        sql = sdscatprintf(sql, "UPDATE market SET closing_price = '%s', "
                                "update_time = CURRENT_TIMESTAMP() WHERE id = %d",
                           closing_price, settings.markets[i].id);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            free(closing_price);
            break;
        }

        sdsfree(sql);
        free(closing_price);
    }
}

static void *on_job_init(void)
{
    return mysql_connect(&settings.db_sys);
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    ((struct job_request*)entry->request)->callback_fn(conn);
}

static void on_job_cleanup(nw_job_entry *entry)
{
    struct job_request *req = entry->request;
    free(req);
}

static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

static void on_min_periodic(nw_periodic *p, void *data)
{
    struct job_request *req = malloc(sizeof(struct job_request));
    memset(req, 0, sizeof(struct job_request));
    req->callback_fn = load_new_assets_market;
    nw_job_add(job, 0, req);
}

static void on_daily_periodic(nw_periodic *periodic, void *data)
{
    struct job_request *req_1 = malloc(sizeof(struct job_request));
    memset(req_1, 0, sizeof(struct job_request));
    req_1->callback_fn = update_market_closing_price;
    nw_job_add(job, 0, req_1);
}

int init_config(const char *path)
{
    json_error_t error;
    json_t *root = json_load_file(path, 0, &error);
    if (root == NULL) {
        printf("json_load_file from: %s fail: %s in line: %d\n", path, error.text, error.line);
        return -__LINE__;
    }
    if (!json_is_object(root)) {
        json_decref(root);
        return -__LINE__;
    }

    int ret = read_config_from_json(root);
    if (ret < 0) {
        json_decref(root);
        return ret;
    }
    json_decref(root);

    MYSQL *conn = mysql_connect(&settings.db_sys);
    ret = load_assets_from_db(conn);
    ret = load_market_from_db(conn);
    mysql_close(conn);

    return 0;
}

int init_job()
{
    nw_job_type type;
    memset(&type, 0, sizeof(type));
    type.on_init    = on_job_init;
    type.on_job     = on_job;
    type.on_cleanup = on_job_cleanup;
    type.on_release = on_job_release;

    job = nw_job_create(&type, 1);
    if (job == NULL)
        return -__LINE__;

    // 9AM KST
    nw_periodic_set(&daily_periodic, 1546819200, 86400, on_daily_periodic, NULL);
    nw_periodic_start(&daily_periodic);

    // Every minute
    nw_periodic_set(&min_periodic, 1545645600, 120, on_min_periodic, NULL);
    nw_periodic_start(&min_periodic);

    return 0;
}
