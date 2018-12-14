/*
 * Description: 
 *     History: yang@haipo.me, 2016/04/19, create
 */

# include <curl/curl.h>
# include "ac_server.h"
# include "ac_config.h"

static nw_svr *svr;
static redis_sentinel_t *redis;
static const char *magic_head = "373d26968a5a2b698045";

static void send_curl_req(const char *msg)
{
    CURL *curl = curl_easy_init();

    json_t *request = json_object();
    json_object_set_new(request, "text", json_string(msg));
    char *request_data = json_dumps(request, 0);
    json_decref(request);

    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_URL, settings.webhook);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(1000));
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_data);

    curl_easy_perform(curl);

    free(request_data);
    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);
}

static int decode_pkg(nw_ses *ses, void *data, size_t max)
{
    char *s = data;
    size_t size = 0;
    for (size_t i = 0; i < max; ++i) {
        if (s[i] == '\n') {
            size = i + 1;
            break;
        }
    }
    if (size != 0) {
        if (size <= 20)
            return -1;
        if (memcmp(data, magic_head, 20) != 0)
            return -2;
        return size;
    }

    return 0;
}

static void on_recv_pkg(nw_ses *ses, void *data, size_t size)
{
    char *message = data;
    message[size - 1] = '\0';
    if (message[size - 2] == '\r')
        message[size - 2] = '\0';
    message += 20;
    log_info("alert message: %s", message);

    redisContext *context = redis_sentinel_connect_master(redis);
    if (context == NULL) {
        log_error("connect redis master fail");
        return;
    }
    redisReply *reply = redisCmd(context, "RPUSH alert:message %s", message);
    if (reply == NULL) {
        log_error("RPUSH message fail");
        redisFree(context);
        return;
    }
    freeReplyObject(reply);
    redisFree(context);

    send_curl_req(message);
}

static void on_error_msg(nw_ses *ses, const char *msg)
{
    log_error("peer: %s error: %s", nw_sock_human_addr(&ses->peer_addr), msg);
}

static int init_svr(void)
{
    nw_svr_type type;
    memset(&type, 0, sizeof(type));
    type.decode_pkg = decode_pkg;
    type.on_recv_pkg = on_recv_pkg;
    type.on_error_msg = on_error_msg;

    svr = nw_svr_create(&settings.svr, &type, NULL);
    if (svr == NULL)
        return -__LINE__;
    if (nw_svr_start(svr) < 0)
        return -__LINE__;

    return 0;
}

static int init_redis(void)
{
    redis = redis_sentinel_create(&settings.redis);
    if (redis == NULL)
        return -__LINE__;
    return 0;
}

int init_server(void)
{
    ERR_RET(init_svr());
    ERR_RET(init_redis());

    return 0;
}

