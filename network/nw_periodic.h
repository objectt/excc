/*
 * Description: 
 *     History: objectt, 2018/12/19, create
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

# endif
