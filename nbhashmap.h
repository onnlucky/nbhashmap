#ifndef _nbhashmap_h_
#define _nbhashmap_h_

/**
 *
 * A non blocking hashmap.
 *
 * A fully thread safe map structure, mapping keys onto values.
 *
 * You create a map by passing in three functions on keys: a hash function, an
 * equals function and a free function. This is because the map needs to know
 * the keys and how to handle them; it "owns" the keys. That is it will free
 * them when needed.
 *
 * The map does not know anything about the values, they are opaque pointers.
 * And the map does not "own" the values. That is, it will never free the
 * values.
 *
 * This data structure does not use global locks and therefor never blocks a
 * thread accessing it. This gives it excellent performance characteristics,
 * even with many threads reading or updating mappings.
 *
 * Everything a thread does before updating a mapping is guarenteed to
 * happen-before another thread reading the updated mapping.
 */

/// public type for a hashmap.
typedef struct HashMap HashMap;

/// Hash function to generate a hash from a key, notice producing a good hash
/// function is paramount for good performance.
typedef unsigned int (hashmap_key_hash)(void *key);

/// Equals function the map must use to compare keys. Notice it is
/// important for this function to behave (run without errors) when
/// the passed in key is corrupt. (@hashmap_key_free has been called
/// on the key.)
typedef int (hashmap_key_equals)(void *left, void *right);

/// A function to free keys when the map no longer uses them.
typedef void (hashmap_key_free)(void *key);


/// Create a new hashmap using a @equals, @hash and @free function.
/// @returns a new hashmap
HashMap * hashmap_new(hashmap_key_equals *equals, hashmap_key_hash *hash, hashmap_key_free *free);

/// Free a hashmap @map. Notice this is not thread safe, so make sure the map
/// is really not in use anymore by any thread. It will free all keys and
/// internal resources. It will not free any still referenced values.
void hashmap_free(HashMap *map);

/// Return the current count of mappings in the @map. Notice, updating a
/// mapping to null is equivalent to deleting it. So only values mapping keys
/// to non-zero values are counted.
int hashmap_size(HashMap *map);


/// Return the current mapping for @key in @map.
/// Notice, unlike the @hashmap_putif, the map does not own the key.
void * hashmap_get(HashMap *map, const void *key);

/// A marker to pass into @hashmap_putif to indicate you don't care about the
/// current mapped value.
extern void *IGNORE;

/// Update the mapping for @key to @val in @map. Notice, the map own's the key
/// you pass in. Also note that passing in null as the new value is equivalent
/// to deleting the mapping. (As all mappings return null if they don't exist.)
///
/// @oldval the value that must be currently in map for the update to succeed;
/// use @IGNORE if the update must always succeed.
void * hashmap_putif(HashMap *map, void *key, const void *val, const void *oldval);

#endif

