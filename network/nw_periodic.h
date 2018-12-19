/*
 * Description: 
 *     History: yang@haipo.me, 2016/03/20, create
 */

# ifndef _NW_periodic_H_
# define _NW_periodic_H_

# include <stdbool.h>

# include "nw_evt.h"

struct nw_periodic;
typedef void (*nw_periodic_callback)(struct nw_periodic *periodic, void *privdata);

typedef struct nw_periodic {
    ev_periodic ev;
    struct ev_loop *loop;
    double interval;
    nw_periodic_callback callback;
    void *privdata;
} nw_periodic;

void nw_periodic_set(nw_periodic *periodic, double offset, double interval, nw_periodic_callback callback, void *privdata);
void nw_periodic_start(nw_periodic *periodic);
void nw_periodic_stop(nw_periodic *periodic);
bool nw_periodic_active(nw_periodic *periodic);
double nw_periodic_remaining(nw_periodic *periodic);

# endif

