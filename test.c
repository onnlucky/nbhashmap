#include "nbhashmap.c"

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#define TCOUNT 5
#define WCOUNT 50000

#define TRANDOM 5
#define TRANGE  200
#define WRAP    200000

// good hash function is essential; this is murmurhash2a
#define mmix(h,k) { k *= m; k ^= k >> r; k *= m; h *= m; h ^= k; }
static unsigned int murmurhash2a(const void * key, int len) {
    const unsigned int seed = 33;
    const unsigned int m = 0x5bd1e995;
    const int r = 24;
    unsigned int l = len;
    const unsigned char * data = (const unsigned char *)key;
    unsigned int h = seed;
    while(len >= 4) {
        unsigned int k = *(unsigned int*)data;
        mmix(h,k);
        data += 4;
        len -= 4;
    }
    unsigned int t = 0;
    switch(len) {
        case 3: t ^= data[2] << 16;
        case 2: t ^= data[1] << 8;
        case 1: t ^= data[0];
    }
    mmix(h,t);
    mmix(h,l);
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}
static unsigned int makehash(void *key) { return murmurhash2a(key, strlen(key)); }

static int equals(const char *left, const char *right) {
    if (left == right) return 1;
    if (left == 0 || right == 0) return 0;
    return strcmp(left, right) == 0;
}
static int keyequals(void *left, void *right) { return equals((const char *)left, (const char *)right); }


// global map to test
static HashMap *map;
static volatile long around = 0;

static void maybe_yield() {
    if (random() % 100 == 0) yield();
}

void * hammer(void *data) {
    int tid = (long)data;
    print("launching tid: %d", tid);

    char buf[100];
    for (int i = 0; i < WCOUNT; i++) {
        snprintf(buf, 100, "value: [%d]-%d", tid, i);
        const char *key = strdup(buf);
        //print("put: [%d] - key %d", tid, i);
        hashmap_putif(map, strdup(key), (void *)key, IGNORE);
        maybe_yield();
    }

    print("almost done tid: %d, %ld", tid, hashmap_size(map));

    for (int i = 0; i < WCOUNT; i++) {
        snprintf(buf, 100, "value: [%d]-%d", tid, i);
        const char *val = hashmap_get(map, buf); val = val;
        // not with deleters around
        //assert(val);
        //assert(0 == strcmp(buf, hashmap_get(map, buf)));

        //hashmap_putif(map, val, null, IGNORE);
        //free((char *)val);
    }

    print("done tid: %d, %ld", tid, hashmap_size(map));
    return null;
}

void * hammerrandom(void *data) {
    int tid = (long)data;
    print("launching random tid: %d", tid);
    char buf[100];

    for (int i = 0; i < WCOUNT; i++) {
        int n = around + random() % TRANGE;
        snprintf(buf, 100, "%d", n);
        const char *key = strdup(buf);
        maybe_yield();
        if (0 == random() % 5) {
            around = (around + 1) % WRAP;
            void *old = (void *)hashmap_putif(map, strdup(key), (void *)key, IGNORE);
            if (old) free(old);
            //print("%02d - put: %s", tid, key);
        } else {
            void *old = (void *)hashmap_putif(map, strdup(key), null, IGNORE);
            free((void *)key);
            if (old) free(old);
            //print("%02d - del: %s", tid, key);
        }
    }
    return null;
}

static volatile int stopping = 0;
void * deleter(void *data) {
    int tid = (long)data;
    print("launching deleter tid: %d", tid);

    while (1) {
        if (stopping) {
            if (stopping == 2) {
                print("stopping deleter tid: %d", tid);
                return null;
            }
            stopping = 2; // make sure we go another round to really delete maps
        }

        usleep(500);
        header *kvs = getkvs(map);
        unsigned int len = kvs->len;

        for (int i = 0; i < len; i++) {
            entry *e = _load(kvs, i);
            const char *k = getkey(e);
            const char *v = getval(e);
            if (k && k != SIZED && v && v != SIZED) {
                if (!stopping) maybe_yield();
                const char * old = hashmap_putif(map, strdup(k), null, IGNORE);
                if (old) free((char *)old);
            }
        }
        if (tid) return null;
    }
    return null;
}

void * tester(void *data) {
    int tid = (long)data;
    print("launching tester tid: %d", tid);
    hashmap_putif(map, strdup("probe1"), "probe1", IGNORE);
    hashmap_putif(map, strdup("probe2"), "probe2", IGNORE);
    hashmap_putif(map, strdup("probe3"), "probe3", IGNORE);
    hashmap_putif(map, strdup("123test"), "123test", IGNORE);

    while (1) {
        usleep(5000);

        void *probe = hashmap_get(map, "probe1");
        if (!probe || strcmp(probe, "probe1")) fatal("AUCH");
        hashmap_putif(map, strdup("probe1"), "XXX", probe);
        hashmap_putif(map, strdup("probe1"), "YYY", probe);
        probe = hashmap_get(map, "probe1");
        if (!probe || strcmp(probe, "XXX")) fatal("AUCH");
        hashmap_putif(map, strdup("probe1"), "probe1", probe);

        assert(strcmp(hashmap_get(map, "probe1"), "probe1") == 0);
        assert(strcmp(hashmap_get(map, "probe2"), "probe2") == 0);
        assert(strcmp(hashmap_get(map, "probe3"), "probe3") == 0);
        assert(strcmp(hashmap_get(map, "123test"), "123test") == 0);
        assert(hashmap_get(map, "something") == null);
        //print("tester: ok");
        if (stopping) {
            print("stopping tester tid: %d", tid);
            return null;
        }
    }
    return null;
}

void freekey(void *key) {
    print("FREEING: %s", (const char *)key);
    free(key);
}

int main(int argc, char **argv) {
    print("starting...");

    map = hashmap_new(keyequals, makehash, free);
    hashmap_putif(map, strdup("hello world"), "bye world", IGNORE);
    hashmap_putif(map, strdup("hello world"), "see you soon", IGNORE);
    print("%ld", hashmap_size(map));
    assert(hashmap_size(map) == 1);
    hashmap_putif(map, strdup("hello world"), null, IGNORE);
    assert(hashmap_size(map) == 0);
    hashmap_putif(map, strdup("foo1"), null, IGNORE);
    hashmap_putif(map, strdup("foo2"), null, IGNORE);
    hashmap_putif(map, strdup("foo3"), null, IGNORE);
    hashmap_putif(map, strdup("foo4"), null, IGNORE);
    hashmap_putif(map, strdup("foo1"), "bar", IGNORE);
    hashmap_putif(map, strdup("foo2"), "bar", IGNORE);
    hashmap_putif(map, strdup("foo3"), "bar", IGNORE);
    hashmap_putif(map, strdup("foo4"), "bar", IGNORE);
    hashmap_putif(map, strdup("foo1"), null, IGNORE);
    hashmap_putif(map, strdup("foo2"), null, IGNORE);
    hashmap_putif(map, strdup("foo3"), null, IGNORE);
    hashmap_putif(map, strdup("foo4"), null, IGNORE);
    assert(hashmap_size(map) == 0);
    //hashmap_free(map);
    //return 0;

    hashmap_putif(map, strdup("hello world"), strdup("bye world"), IGNORE);

    pthread_t tmp;
    //pthread_create(&tmp, null, &deleter, null);
    pthread_create(&tmp, null, &tester, null);
    usleep(200);

    print("launching hammers: %d", TCOUNT);
    pthread_t threads[TCOUNT];
    for (long i = 0; i < TCOUNT; i++) {
        int r = pthread_create(&threads[i], null, &hammer, (void *)i);
        if (r) fatal("pthread_create: %d", r);
    }

    print("launching random: %d", TRANDOM);
    pthread_t randoms[TRANDOM];
    for (long i = 0; i < TRANDOM; i++) {
        int r = pthread_create(&randoms[i], null, &hammerrandom, (void *)i);
        if (r) fatal("pthread_create: %d", r);
    }

    sleep(2);
    assert(0 == strcmp(hashmap_get(map, "hello world"), "bye world"));

    for (int i = 0; i < TCOUNT; i++) pthread_join(threads[i], null);
    for (int i = 0; i < TRANDOM; i++) pthread_join(randoms[i], null);

    hashmap_debug(map);
    sleep(1);

    stopping = 1;
    pthread_join(tmp, null);
    print("alone again... its all so quiet...: %ld", hashmap_size(map));

    //assert(hashmap_size(map) == 0);
    hashmap_free(map);
    print("DONE DONE DONE");
    return 0;
}

