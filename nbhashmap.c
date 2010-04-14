// ** a non blocking hash map **
//
// author: Onne Gorter <onne@onnlucky.com>
// version: 2010-04-13
// depends: libatomic-ops
// license: MIT; see license.txt
//
// inspired by Cliff Clicks Java version; I've simplified the state machine and other algorithms
//
// internal state machine; keys map to null if they 1) don't exist in map; or 2) are updated to null (== deleted)
// notice the states must be read (and written) in key, hash, value order (using a true happens-before fashion)
// key, hash, value = <state name> -> ("transition name": <new state>)*
// 0, _, _ = free        -> "claim": partial, "resize empty": sized-free
// k, h, v = value       -> "update": value, "resize value": sized-value
// S, _, _ = sized-free  -> "restart"
// k, h, S = sized-value -> "restart"
// k, 0, _ = partial     -> "wait hash": value
//
// Notice, though it is a _non blocking_ hash map, it does sometimes yield (sleep) the current thread. But only if
// another thread promised to do something, but was unscheduled before (or is still busy) doing it. Since these
// promises will happen in a near future without any blocking, this is ok. Unfortunately we cannot know the identity
// of the promising thread, and we cannot yield only until the promise is fullfilled.
// (A signal-when-memory-written-or-is-not(oldval) ... might be a nice primitive to complement cas; futex?)
//
// Situations where a thread yields:
// * waiting for a hash to appear, after we raced to observe a previously not claimed key slot
// * when the map needs resizing, we yield to the winner of the resize to publish new (uninitialized) map
// * when helping zeroing or copying, we yield to wait until all other helpers are done
// * after resize, we yield until new map is promoted to current map (by the winner of the second case)
//
// One danger! When resizing a map, we delete (and free) keys mapping to null. However another thread using the old
// table might still do equals() on the key we just free'd. The result of this equals doesn't matter (as long as it
// finishes somehow), since it will read a SIZED for the value. But, if the free also unmaps the page the key resides
// on, we will generate a page fault. (Plus the equals function might get passed in a corrupt key, which might not be
// expected and cause trouble on its own.) Maybe something needs to be done about this ...
//
//
// TODO some false sharing might be going on, especially _btodo and _bdone fields
// TODO yield is great on macosx, but on linux it is horrible, but so is any kind of sleep ... unless time ./test lies
// TODO we don't need to volatile read key,hash,value ... think about that (at least key and hash are final)
// TODO a shrinking map might want to resize into something smaller, how and when and why?
// TODO add more public api, iterators and such, and a delete that doesn't own the key ... (pass in free function to _putif?)
// TODO allow null functions for hash/equals/free when: key == hash, equals == value compare, free == nop
// TODO add support for garbage collectors and fixed Values or such... as compile time option/macros maybe?
// TODO refactor _zero_block and _copy_block; they share a lot of code
// TODO handle out of memory ... but we really cannot do anything sensible
// TODO think about how to handle deleted keys that we free, it is not truly safe this way
// TODO on processors where volatile read/writes do not guarentee a fence, we might have to add more memory fences
// TODO for _get, we don't need do a full circle before return null; since _put will never put anything beyond reprobe limit

#define _GNU_SOURCE
#include <unistd.h>
#include <atomic_ops.h>
#include <sys/time.h>
#include <strings.h>
#include <sched.h>

#define HAVE_DEBUG
#define HAVE_STRACE

#include "debug.h"

#undef STRACE
#define STRACE 0

// threading primitives
static void yield() { sched_yield(); }
static void read_barrier() { AO_nop_read(); }
static void write_barrier() { AO_nop_write(); }
static int cas(void *addr, const void *nval, const void *oval) {
    return AO_compare_and_swap(addr, (AO_t)oval, (AO_t)nval);
}


// ** actual implementation **

typedef struct entry entry;
struct entry {
    volatile void *_key;
    volatile void *_val;
    volatile unsigned int _hash;
};

typedef struct header header;
struct header {
    volatile AO_t _btodo;   // unsigned long; _btodo and _bdone are placed apart to prevent false cachline sharing
    unsigned long len;      // final unsigned long
    header *prev;           // a linked list of older maps to free later
    volatile AO_t _bdone;   // unsigned long
    entry kvs[0];           // the actual entries
};


// the map "owns" the keys, but it needs an equals, hash and free function
typedef int (hashmap_key_equals)(void *left, void *right);
typedef unsigned int (hashmap_key_hash)(void *key);
typedef void (hashmap_key_free)(void *key);

typedef struct HashMap HashMap;
struct HashMap {
    volatile AO_t _size;           // unsigned long
    volatile unsigned int changes; // counting all map modifications; but dropping some read/writes is ok
    volatile header *_kvs;         // the main map
    volatile header *_nkvs;        // the new map when a resize is in flight, so other threads can help

    hashmap_key_equals *equals_func;
    hashmap_key_hash   *hash_func;
    hashmap_key_free   *free_func;
};

#define INITIAL_SIZE 4
#define REPROBE_LIMIT 17
#define BLOCK_SIZE (1024 * 8)

#define null 0                        // indicates value is deleted
static void *SIZED   = "__SIZED__";   // marker to indicate map is or has resized
static void *IGNORE  = "__IGNORE__";  // marker to indicate old map value is to be ignored
static void *DELETED = "__DELETED__"; // marker to indicate key is to be deleted (when resizing)


// when racing to resize, the winner must succesfully cas this into map->nkvs
static header * kvs_promise = (header *)1;

static header * header_new(unsigned int len) {
    header *h = malloc(sizeof(header) + sizeof(entry) * len);
    assert(h);
    h->len = len;
    h->_btodo = 0;
    h->_bdone = 0;
    h->prev = 0;
    return h;
}

static unsigned long current_time() { // return time in seconds
    struct timeval time;
    gettimeofday(&time, 0);
    return time.tv_sec;
}

// link in an old kvs struct, we hold on to it because not all threads might be done with it
static void push_old_kvs(header *nkvs, header *okvs) {
    nkvs->prev = okvs;
    okvs->_btodo = current_time(); // we just reuse this field
}

// free all kvs older than cutoff
static int free_old_kvs2(header *kvs, unsigned long cutoff) {
    if (!kvs) return 1;
    if (free_old_kvs2(kvs->prev, cutoff)) {
        kvs->prev = 0;
        if (kvs->_btodo < cutoff) {
            free(kvs);
            return 1;
        }
    }
    return 0;
}

// try to free some older maps
static void free_old_kvs(header *nkvs) {
    unsigned long cutoff = current_time() - 30; // seconds
    if (free_old_kvs2(nkvs->prev, cutoff)) {
        nkvs->prev = 0;
    }
}

// these functions read from volatile memory, we should really do that only once per "need"
inline static entry * _load(header *kvs, int idx) {
    assert(idx >= 0);
    assert(idx < kvs->len);
    return kvs->kvs + idx;
}

inline static header * getkvs(HashMap *map) { return (header *)map->_kvs; }

inline static void * getkey(entry *e) { return (void *)e->_key; }
inline static void * getval(entry *e) { return (void *)e->_val; }
inline static unsigned int gethash(entry *e) {
    unsigned int h = e->_hash;
    // this corresponds to the "wait hash" transition:
    // another thread just claimed a key, but did not yet come around to writing the hash for it
    while (!h) {
        yield(); h = e->_hash; // since these fields are volatile, this will go read from main memory
    }
    return h;
}

/// create a new map
HashMap * hashmap_new(hashmap_key_equals *equals_func, hashmap_key_hash *hash_func, hashmap_key_free *free_func) {
    assert(sizeof(unsigned long) <= sizeof(AO_t));

    HashMap *map = malloc(sizeof(HashMap));
    map->_size = 0;
    map->changes = 0;
    map->equals_func = equals_func;
    map->hash_func = hash_func;
    map->free_func = free_func;

    header *kvs = header_new(INITIAL_SIZE);
    bzero(kvs->kvs, sizeof(entry) * INITIAL_SIZE);

    map->_kvs = kvs;
    map->_nkvs = 0;
    return map;
}

static void free_kvs2(header *kvs) { // just free all old kvs
    if (kvs == 0) return;
    free_kvs2(kvs->prev);
    free(kvs);
}

// freeing the top level map; notice we cannot free the values
static void free_kvs(HashMap *map, header *kvs) {
    free_kvs2(kvs->prev);
    for (int i = kvs->len - 1; i >= 0; i--) {
        entry *e = _load(kvs, i);
        void *k = getkey(e);
        assert(k != SIZED);
        if (k) map->free_func(k);
    }
    free(kvs);
}

/// free a @map, be careful not to free a map still in use
/// Also note the values will not be free'd, they never belong to the hashmap in the first place.
void hashmap_free(HashMap *map) {
    strace("freeing hashmap: %p", map);
    free_kvs(map, getkvs(map));
    free(map);
}

/// return the current size of the @map
long hashmap_size(HashMap *map) {
    long res = map->_size;
    // notice it is "normal" for size to drop below zero sometimes (temporarily):
    // if we just had many deletes and adds, but more _size_updates are still "in flight" for the latter
    if (res < 0) return 0;
    return res;
}

static void _size_update(HashMap *map, int n) {
    AO_fetch_and_add(&map->_size, n);
}

static void * _putif(HashMap *map, int resizing, header *kvs, void *key, const unsigned int hash, void *val, void *oldval);

// when resizing, any thread can claim the next block of the new map and zero it
int _zero_block(header *nkvs) {
    assert(nkvs); assert(nkvs->len);
    unsigned long len = nkvs->len;
    unsigned int todo = 1 + (len - 1) / BLOCK_SIZE;
    assert(todo > 0);
    if (len <= BLOCK_SIZE) assert(todo == 1);

    unsigned long block = AO_fetch_and_add(&nkvs->_btodo, 1);
    if (block >= todo) { // done with work, wait for all workers to finish
        while (nkvs->_bdone < todo) yield(); // yield while waiting
        return 0;        // done
    }

    unsigned int blen = BLOCK_SIZE;
    if (block * BLOCK_SIZE + BLOCK_SIZE > len) blen = len - block * BLOCK_SIZE;

    //strace("[%p]: zeroing(%lu): %p: %lu - %u", pthread_self(), block, nkvs, block * BLOCK_SIZE, blen);
    bzero(nkvs->kvs + block * BLOCK_SIZE, sizeof(entry) * blen);

    unsigned long bdone = AO_fetch_and_add(&nkvs->_bdone, 1);
    if (bdone >= todo) return 0; // done
    return 1;                    // more work todo
}

// when resizing, any thread can claim the next block of the old map and copy it
static int _copy_block(HashMap *map, header *okvs, header *nkvs) {
    assert(map); assert(okvs); assert(nkvs); assert(nkvs != kvs_promise);
    unsigned long len = okvs->len;
    unsigned int todo = 1 + (len - 1) / BLOCK_SIZE;
    assert(todo > 0);
    if (len <= BLOCK_SIZE) assert(todo == 1);

    unsigned long block = AO_fetch_and_add(&okvs->_btodo, 1);
    if (block >= todo) { // done with work, wait for all workers to finish
        while (okvs->_bdone < todo) yield(); // yield while waiting
        return 0;        // done
    }

    unsigned long blen = BLOCK_SIZE;
    if (block * BLOCK_SIZE + BLOCK_SIZE > len) blen = len - block * BLOCK_SIZE;
    blen = block * BLOCK_SIZE + blen;

    //strace("[%p]: copying: %p: %lu - %lu", pthread_self(), okvs, block * BLOCK_SIZE, blen);
    for (int i = block * BLOCK_SIZE; i < blen; i++) {
        entry *e = _load(okvs, i);
        while (1) {
            void *k = getkey(e);
            if (k) {
                // found a key to move, mark it as SIZED, and copy it to new map, or delete it if it maps to null
                void *old = getval(e);
                if (cas(&e->_val, SIZED, old)) {
                    if (DELETED == _putif(map, 1, nkvs, k, gethash(e), old, null)) {
                        // deleted key; we no longer need this key; some threads might still want to compare it, so first mark the slot as sized
                        if (!cas(&e->_key, SIZED, k)) fatal("marking deleted key");
                        // aha; we would like this to be perfectly safe, but it really isn't ... it is 99.9999% safe ...
                        // if the free also unmaps the page the key resides in, another thread still doing a key compare will segfault
                        // other than that it hardly matters, since results of such racy equals_func don't matter
                        map->free_func(k);
                    }
                    break;
                } else {
                    strace("we lost race for: %d; retry", i);
                }
            } else {
                if (cas(&e->_key, SIZED, null)) {
                    break;
                } else {
                    strace("we lost race for empty slot: %d; retry", i);
                }
            }
        }
    }

    unsigned long bdone = AO_fetch_and_add(&okvs->_bdone, 1);
    if (bdone >= todo) return 0; // done
    return 1;                    // more work todo
}

void * _resize(HashMap *map, header *okvs);

// when a resize is detected, try to help it along
void _help_resize(HashMap *map, header *okvs) {
    if (map->_kvs != okvs) return;

    strace("help resize: %p, %p", map->_kvs, okvs);
    header *nkvs = (header *)map->_nkvs;
    while (nkvs == 0 || nkvs == kvs_promise) {
        if (map->_kvs != okvs) return;
        if (nkvs == 0) { // try to start a resize ourselves; this compensates for late promises
            _resize(map, okvs);
            return;
        }
        yield(); nkvs = (header *)map->_nkvs;
    }

    while (map->_kvs == okvs && _zero_block(nkvs));
    while (map->_kvs == okvs && _copy_block(map, okvs, nkvs));
    while (map->_kvs == okvs) yield(); // yield until a new map is promoted to current
    strace("done: %p, %p", map->_kvs, okvs);
}

// when we need to resize
void * _resize(HashMap *map, header *okvs) {
    assert(map);
    strace("maybe resize: %p, %p, %p", map->_kvs, okvs, map->_nkvs);
    if (map->_nkvs != null) return SIZED; // somebody else already produced a new map
    if (map->_kvs != okvs) return SIZED;  // somebody else alreay promoted a new map

    if (cas(&map->_nkvs, kvs_promise, null)) {
        if (map->_kvs != okvs) {
            if (!cas(&map->_nkvs, null, kvs_promise)) fatal("unpublising late promise");
            return SIZED; // we are so late: we didn't actually win the race; the winner already moved on
        }

        // we won the race to produce new map
        int size = hashmap_size(map);
        unsigned int len = okvs->len;

        // calculate how large we want next map to be
        header *nkvs = null;
        if (map->changes > (len / 4) && size / (float)len < 0.3f) {
            // if there have been plenty mutations, and our full ration is pretty bad, just copy to remove garbage
            strace("resizing to remove garbage: %d", len);
            nkvs = header_new(len);
        } else {
            strace("resizing: %d (%d <= %d && %.2f >= 0.3)", len * 2, map->changes, (len / 4), size / (float)len);
            nkvs = header_new(len * 2);
        }
        assert(nkvs); assert(nkvs->len);
        // when racing on many resizes, some threads doing _zero_block might loop until _bdone >= todo
        // and we reset it to zero here; not such a big deal, since it will become >= todo after _copy_block
        okvs->_btodo = 0;
        okvs->_bdone = 0;

        write_barrier();  // publish results
        map->_nkvs = nkvs; // expose new map so others can help

        while (_zero_block(nkvs));
        while (_copy_block(map, okvs, nkvs));

        // here we could free the map, but many threads might still need to read the SIZED markers
        // so we keep all old lists and free only the really old; with a gc this is much better
        push_old_kvs(nkvs, okvs);
        free_old_kvs(nkvs);

        // this is the required order: otherwise another thread might attempt to resize (when compensating for late promise)
        // notice we compensate that we can now observe nkvs == kvs (in _putif)
        if (!cas(&map->_kvs, nkvs, okvs))  fatal("publishing new map");
        if (!cas(&map->_nkvs, null, nkvs)) fatal("unpublising resize in progress");
        map->changes = 0;
        strace("done resizing: %p[%lu].size: %ld", nkvs, nkvs->len, hashmap_size(map));
        return SIZED; // always indicate we need to retry after resize
    }

    // we lost the race to produce new map
    return SIZED;
}

static void * _get(HashMap *map, header *kvs, void *key, const unsigned int hash) {
    const unsigned int len = kvs->len;
    int idx = hash & (len - 1);

    int reprobe_try = 0;
    while (1) {
        entry *e = _load(kvs, idx);
        void *k = getkey(e);
        if (k == 0) return 0;         // finding an empty slot indicates the mapping doesn't exist
        if (k == SIZED) return SIZED; // finding a SIZED slot indicates a map resize is in flight

        unsigned int h = gethash(e);  // first check memoized hash, before doing full key compare
        if (h == hash) {
            read_barrier();           // needed to ensure we can read the other key fully
            if (map->equals_func(k, key)) {
                return getval(e);     // keys are equal, we found our mapping
            }
        }

        if (++reprobe_try >= len) return 0; // going full circle, we know the mapping does not exist
        idx = (idx + 1) & (len - 1);        // try next slot
    }
}

static void * _putif(HashMap *map, int resizing, header *kvs, void *key, const unsigned int hash, void *val, void *oldval) {
    assert(map); assert(kvs);
    const unsigned int len = kvs->len;
    int idx = hash & (len - 1);
    int mustfreekey = 0; // used to mark if passed in key must be freed; if we return SIZED, we want to reuse the key...

    assert(key); assert(hash);
    strace("%p %p :: [%s] = %s old: %s", map, kvs, (const char *)key, (const char *)val, (const char *)oldval);


    // first we try to find the slot to update, or claim a new one
    int reprobe_try = 0;
    entry *e;
    while (1) {
        e = _load(kvs, idx);
        void *k = getkey(e);

        if (k == null) { // we found an unclaimed slot; try to claim it
            if (val == null && (oldval == IGNORE || oldval == null)) {
                // this means we are deleting a mapping that doesn't exit; so we don't have to do anything
                if (resizing) return DELETED; // when resizing, signal the key must be free'd
                // just make sure it is still really null before returning null
                if (cas(&e->_key, null, null)) {
                    map->free_func(key);      // we no longer need the given key
                    return null;
                }
            }

            write_barrier();     // needed to ensure others can read our key fully
            if (cas(&e->_key, key, null)) {
                e->_hash = hash; // so we claimed the slot, write the key
                break;           // and go on to writing the value
            }
            // we couldn't claim the empty slot, ensure we reread the no longer null key
            // TODO if cas returned the new pointer, we didn't have to do this extra memory read
            k = getkey(e);
        }

        assert(k);
        if (k == SIZED) return SIZED;  // map is resizing
        unsigned int h = gethash(e);
        if (h == hash) {
            read_barrier();            // needed to ensure we can read the other key fully
            if (map->equals_func(k, key)) { // keys are equal, we found the spot where we must update the value
                mustfreekey = 1;       // mark that key should be deleted
                break;
            }
        }

        // if no map, we are in a resize; never return _resize when already resizing
        if (!resizing && ++reprobe_try >= REPROBE_LIMIT) return _resize(map, kvs);
        idx = (idx + 1) & (len - 1);   // try next stot
    }


    // second we try to update the slots value
    void *v = getval(e);               // first read the old value
    if (v == SIZED) return SIZED;
    if (!resizing && v != null) {
        // we quickly check if resize is in progress, to prevent wasting effort on old map
        header *nkvs = (header *)map->_nkvs;
        if (nkvs != 0 && nkvs != kvs) return SIZED;
        if (map->_kvs != kvs) return SIZED;
    }

    while (1) {
        if (oldval != IGNORE && v != oldval) {
            // we cannot update value, because it doesn't match passed in given value
            if (resizing) fatal("resize: %s = %p != %p new: %p", (const char *)key, v, oldval, val);
            return v; // return the current value
        }

        if (cas(&e->_val, val, v)) {
            // we won the race to update the value; update map->size as needed
            if (!resizing && v == null && val != null) _size_update(map, 1);
            if (!resizing && v != null && val == null) _size_update(map, -1);
            if (!resizing) map->changes++;

            if (mustfreekey) map->free_func(key); // we no longer need the given key
            return v;                             // return the previous value we just replaced
        }

        // we lost the race to update; try again with updated value
        // TODO if cas returned the new pointer, we didn't have to do this extra memory read
        v = getval(e);
        if (v == SIZED) return SIZED;  // map is resizing
    }
}


/// return the current mapping for @key
/// @map the map to query
/// @key the key for the value; the map will not own nor free this key
void * hashmap_get(HashMap *map, void *key) {
    unsigned int hash = map->hash_func(key);
    if (!hash) hash = 1; // we cannot have 0 as a hash value

    header *kvs = getkvs(map);
    void *res = _get(map, kvs, key, hash);
    while (res == SIZED) {
        _help_resize(map, kvs);
        kvs = getkvs(map);
        res = _get(map, kvs, key, hash);
    }
    return res;
}

/// update the mapping for @key to @val
/// @map    the map to update
/// @key    the key which mapping to update; the map owns this key and will free it when needed
/// @val    the new value to put in map
/// @oldval the value that must be currently in map for the update to succeed; use @IGNORE if the update must always succeed
void * hashmap_putif(HashMap *map, void *key, const void *val, const void *oldval) {
    unsigned int hash = map->hash_func(key);
    if (!hash) hash = 1;

    header *kvs = getkvs(map);
    void *res = _putif(map, 0, kvs, key, hash, (void *)val, (void *)oldval);
    while (res == SIZED) {
        _help_resize(map, kvs);
        kvs = getkvs(map);
        res = _putif(map, 0, kvs, key, hash, (void *)val, (void *)oldval);
    }
    return res;
}

/// print some debugging info about the @map
void hashmap_debug(HashMap *map) {
    const int len = getkvs(map)->len;
    const int size = hashmap_size(map);

    float ratio = size / (float)len;
    float mb = (sizeof(entry) * len) / (float) (1024 * 1024);
    print("%f (%d / %d) = %.0fmb", ratio, size, len, mb);
}

