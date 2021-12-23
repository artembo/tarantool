/*
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "thread.h"
#include "init.h"
#include "../../lua/init.h"
#include "fiber.h"
#include "../../lua/error.h"
#include "../error.h"
#include "../../lua/utils.h"

#include "../../lua/fiber.h"

#include "../space.h"
#include "space.h"
#include "schema.h"
#include "../../lib/core/fiber.h"
#include "../../lib/core/cbus.h"

#include "../wal.h"

static const char *threadlib_name = "box.thread";

struct box_thread {
	/** Freshly new lua state for this thread. */
	struct lua_State *state;
	struct cord cord;
	struct rlist in_idle;
	struct rlist in_active;
	/** A pipe from this thread to WAL. */
	struct cpipe wal_pipe;
	/**
	 * A pipe from WAL to this thread. Analogue of tx_prio pipe in
	 * wal_writer.
	 */
	struct cpipe box_pipe;
};

struct box_thread_pool {
	size_t size;
	/** Array of box threads. */
	struct box_thread *threads;
	struct rlist idle_threads;
	/** List to find thread by name in wal_write. */
	struct rlist active_threads;
};

enum {
	BOX_THREAD_COUNT_DEFAULT = 4,
};

struct box_thread_pool *box_thread_pool = NULL;

static void
box_init_thread_pool()
{
	if (box_thread_pool != NULL)
		return;
	size_t pool_sz = sizeof(struct box_thread_pool) +
			 BOX_THREAD_COUNT_DEFAULT * sizeof(struct box_thread);
	box_thread_pool =
		(struct box_thread_poll *) malloc(pool_sz);
	if (box_thread_pool == NULL)
		panic("Failed to allocated box thread pool!");
	box_thread_pool->size = BOX_THREAD_COUNT_DEFAULT;
	rlist_create(&box_thread_pool->idle_threads);
	for (size_t i = 0; i < box_thread_pool->size; ++i) {
		struct box_thread *thread = &box_thread_pool->threads[i];
		thread = *(&box_thread_pool + sizeof(struct box_thread_pool) +
			   i * sizeof(struct box_thread));
		cpipe_create(&thread->wal_pipe, "wal");
		cpipe_set_max_input(&thread->wal_pipe, IOV_MAX);
		rlist_add_tail_entry(&box_thread_pool->threads, thread, in_threads);
	}
}

void
box_init_thread()
{
	for (size_t i = 0; i < box_thread_pool->size; ++i) {
		/* Endpoint for WAL thread. */
		struct cbus_endpoint endpoint;
		cbus_endpoint_create(&endpoint, cord_name(&thread->cord),
				     box_thread_cb, &endpoint);
	}
}

/** Should be called after box_init_thread()! */
void
box_init_box_pipes()
{
	for (size_t i = 0; i < box_thread_pool->size; ++i) {
		cpipe_create(&box_thread_pool->threads[i].box_pipe,
			     cord_name(&thread->cord));
	}

}

static struct box_thread *
box_thread_pool_get_thread()
{
	if (rlist_empty(&box_thread_pool->idle_threads))
		return NULL;
	return rlist_first_entry();
}

static void
box_thread_activate(struct box_thread *thread)
{
	rlist_del();
	rlist_add_entry();
}

struct cpipe *
box_thread_get_wal_pipe()
{
	struct cord *current_cord = cord();
	struct box_thread *thread;
	size_t count = 0;
	rlist_foreach_entry(thread, &box_thread_pool->active_threads, in_active) {
		if (strcmp(current_cord->name, thread->cord.name) == 0)
			return &thread->wal_pipe;
	}
	return NULL;
}

struct cpipe *
box_thread_get_box_pipe()
{
	struct cord *current_cord = cord();
	struct box_thread *thread;
	size_t count = 0;
	rlist_foreach_entry(thread, &box_thread_pool->active_threads, in_active) {
		if (strcmp(current_cord->name, thread->cord.name) == 0)
			return &thread->box_pipe;
	}
	return NULL;
}

static void
box_thread_cb(struct ev_loop *loop, ev_watcher *watcher, int events)
{
	(void) loop;
	(void) events;
	struct cbus_endpoint *endpoint = (struct cbus_endpoint *)watcher->data;
	cbus_process(endpoint);
}

static int
box_thread_f(va_list ap)
{
	struct box_thread *thread = va_arg(ap, struct box_thread *);
	struct lua_State *state = thread->state;
	int top = lua_gettop(state);
	if (top != 1)
		say_error("thread.new() must accept only one argument");
	if (! lua_isstring(state, 1))
		say_error("Argument of new thread function is expected to be string");

	const char *load_str = lua_tostring(state, 1);
	if (luaL_loadstring(state, load_str) != 0)
		say_error("failed to load string!");
	if (lua_pcall(state, 0, 0, 0) != 0) {
		say_error("Failed to call function %s", luaT_tolstring(state, -1, NULL));
	}
	say_error("lua pcall has finished!");
	return 0;
}

static int
lua_init_spaces(struct space *space, void *data)
{
	struct lua_State *new_state = (struct lua_State *) data;
	box_lua_space_new(new_state, space);
	return 0;
}

static void
lua_call_cfg(struct lua_State *L)
{
	lua_getfield(L, LUA_GLOBALSINDEX, "box");
	lua_getfield(L, -1, "cfg");
	lua_call(L, 0, 0);
	lua_pop(L, 1); /* box, cfg */
}

static int
lbox_thread_new(struct lua_State *L)
{
	static size_t thread_count = 0;
	/*
	 * In order to avoid races on box_threads let's allow threads to be
	 * created only from main.
	 */
	if (! cord_is_main())
		luaL_error(L, "Can't create box thread from auxiliary thread");

	int top = lua_gettop(L);
	if (top != 1 || ! lua_isstring(L, 1))
		luaL_error(L, "Usage: box.thread.new('function')");

	const char *func = lua_tostring(L, 1);
	/* Create new thread, new lua state within it. */
	struct box_thread *new_thread =
		(struct box_thread *) calloc(1, sizeof(*new_thread));
	if (new_thread == NULL) {
		diag_set(OutOfMemory, sizeof(*new_thread), "malloc",
			 "box_thread");
		return luaT_error(L);
	}
	struct lua_State *new_state = luaL_newstate();
	if (new_state == NULL) {
		diag_set(ClientError, ER_PROC_LUA, "Failed to create new state");
		return luaT_error(L);
	}

	lua_state_init_libs(new_state);
	lua_settop(new_state, 0);
	box_lua_init(new_state);
	lua_call_cfg(new_state);
	space_foreach(lua_init_spaces, (void *) new_state);
	new_thread->state = new_state;

	char name[FIBER_NAME_MAX];
	snprintf(name, sizeof(name), "box_thread_%lu", thread_count++);
	lua_pushstring(new_state, func);

	is_thread_inited = false;
	if (cord_costart(&new_thread->cord, name, box_thread_f, (void *)new_thread) != 0)
		return luaT_error(L);
	return 0;
}

static int
lbox_thread_stop(struct lua_State *L)
{
	(void) L;
//	if (cord_join(&rlist_first())) {
//		panic_syserror("box thread: join has failed");
//	}
	return 0;
}

static int
lbox_thread_list(struct lua_State *L)
{
	lua_newtable(L);
	struct box_thread *thread;
	size_t count = 0;
	rlist_foreach_entry(thread, &box_threads, in_threads) {
		lua_pushstring(L, thread->cord.name);
		lua_rawseti(L, -2, count++);
	}
	return 1;
}

void
box_lua_thread_init(struct lua_State *L)
{
	static const struct luaL_Reg thread_methods[] = {
		{ "new",  lbox_thread_new },
		{ "list", lbox_thread_list },
		{ "stop", lbox_thread_stop },
		{ NULL,   NULL }
	};
	luaL_register_module(L, threadlib_name, thread_methods);
	lua_pop(L, 1);
	/* Should be called once. */
	if (L == tarantool_L) {
		box_init_thread_pool();
	}
}
