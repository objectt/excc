/*
 * Description: 
 *     History: yang@haipo.me, 2016/03/20, create
 */

# include "nw_periodic.h"

static void on_periodic(struct ev_loop *loop, ev_periodic *ev, int events)
{
    struct nw_periodic *periodic = (struct nw_periodic *)ev;
    periodic->callback(periodic, periodic->privdata);
}

void nw_periodic_set(nw_periodic *periodic, double offset, double interval, nw_periodic_callback callback, void *privdata)
{
    nw_loop_init();
    ev_periodic_init(&periodic->ev, on_periodic, offset, interval, 0);
    periodic->loop = nw_default_loop;
    periodic->interval = interval;
    periodic->callback = callback;
    periodic->privdata = privdata;
}

void nw_periodic_start(nw_periodic *periodic)
{
    if (!ev_is_active(&periodic->ev)) {
        ev_periodic_start(periodic->loop, &periodic->ev);
    }
}

bool nw_periodic_active(nw_periodic *periodic)
{
    if (ev_is_active(&periodic->ev)) {
        return true;
    }
    return false;
}
