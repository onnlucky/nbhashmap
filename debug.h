// author: Onne Gorter <onne@onnlucky.com>

#ifndef _debug_h_
#define _debug_h_

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>

static void _abort() {
#if 1
    void *bt[128];
    char **strings;
    size_t size;

    size = backtrace(bt, 128);
    strings = backtrace_symbols(bt, size);

    fprintf(stderr, "\nfatal error; backtrace:\n");
    for (int i = 0; i < size; i++) {
        fprintf(stderr, "%s\n", strings[i]);
    }
    fflush(stderr);
    free(strings);
#endif
    abort();
}

#define print(f, x...) do { fprintf(stdout, f"\n", ##x); fflush(stdout); } while(0)
#define eprint(f, x...) do { fprintf(stderr, f"\n", ##x); fflush(stderr); } while(0)
#define TODO() do { fprintf(stderr, "%s:%s:%u - not implemented\n", __FILE__, __FUNCTION__, __LINE__); _abort(); } while(0)
#define fatal(f, x...) do { fprintf(stdout, "FATAL: "f"\n", ##x); fflush(stdout); _abort(); } while(0)

#define STRACE 0

/* DEBUGing macros */
#ifdef HAVE_DEBUG

#undef print
#undef eprint
#undef fatal

#define print(f, x...) do { fprintf(stdout, "%s:%s:%u - "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x); fflush(stdout); } while(0)
#define eprint(f, x...) do { fprintf(stderr, "%s:%s:%u - "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x); fflush(stdout); } while(0)
#define fatal(f, x...) do { fprintf(stderr, "FATAL: %s:%s:%u - "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x); _abort(); } while(0)

//#ifndef HAVE_STRACE
//#define HAVE_STRACE
//#endif

#ifndef HAVE_ASSERTS
#define HAVE_ASSERTS
#endif

//#ifndef HAVE_API_STRACE
//#define HAVE_API_STRACE
//#endif

#ifndef HAVE_API_ASSERTS
#define HAVE_API_ASSERTS
#endif

#define warning(f, x...) fprintf(stderr, "WARNING: %s:%s:%u - "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x)
#define debug(f, x...) fprintf(stderr, "debug: %s:%s:%u - "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x)
#define ddebug(f, x...) fprintf(stderr, "debug: %s:%s:%u - "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x)
#define dddebug(f, x...) fprintf(stderr, "debug: %s:%s:%u - "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x)
#define if_debug(t) t

#else // no HAVE_DEBUG

#define warning(f, x...) fprintf(stderr, "warning: "f"\n", ##x)
#define debug(f, x...)
#define ddebug(f, x...)
#define dddebug(f, x...)
#define if_debug(t)

#endif // HAVE_DEBUG

/* STRACing macros */
#ifdef HAVE_STRACE
#ifndef HAVE_API_ASSERTS
#define HAVE_API_ASSERTS
#endif

#define strace(f, x...) if (STRACE) fprintf(stderr, "strace: %s:%s:%u: "f"\n", __FILE__, __FUNCTION__, __LINE__, ##x)
#define strace_create(f, x...) if (STRACE) fprintf(stderr, "strace: %s create: "f"\n", __FUNCTION__, ##x)
#define strace_free(f, x...)   if (STRACE) fprintf(stderr, "strace: %s free: "f"\n", __FUNCTION__, ##x)
#define strace_enter(f, x...)  if (STRACE) fprintf(stderr, "strace: %s enter: "f"\n", __FUNCTION__, ##x)
#define strace_return(f, x...) if (STRACE) fprintf(stderr, "strace: %s returning: "f"\n", __FUNCTION__, ##x)

#else // no HAVE_STRACE

#define strace(f, x...)
#define strace_create(f, x...)
#define strace_free(f, x...)
#define strace_return(f, x...)

#endif // HAVE_STRACE

/* HAVE_API_ASSERTS STRACing macros */
#ifdef HAVE_API_STRACE
#ifndef HAVE_API_ASSERTS
#define HAVE_API_ASSERTS
#endif

#define api_strace(f, x...) fprintf(stderr, "call: %s : "f"\n", __FUNCTION__, ##x)

#else // no HAVE_API_STRACE

#define api_strace(f, x...)

#endif

/* HAVE_ASSERTS macros */
#ifdef HAVE_ASSERTS

#ifndef HAVE_API_ASSERTS
#define HAVE_API_ASSERTS
#endif

#undef assert
#define assert(t) if (! (t) ) { fprintf(stderr, "%s:%s:%u - assertion failed: "#t"\n", __FILE__, __FUNCTION__, __LINE__); _abort(); }

#else // no HAVE_ASSERTS

#undef assert
#define assert(t, x...)

#endif // HAVE_ASSERTS

/* HAVE_API_ASSERTS macros */
#ifdef HAVE_API_ASSERTS

#define api_assert(t, f, x...) if (! (t) ) { fprintf(stderr, "%s:%s:%u - assertion failed: "#t" ", __FILE__, __FUNCTION__, __LINE__); fprintf(stderr, f"\n", ##x); _abort(); }

#else // no HAVE_API_ASSERTS

#define api_assert(t, f, x...)

#endif // HAVE_API_ASSERTS

#endif // _debug_h_

