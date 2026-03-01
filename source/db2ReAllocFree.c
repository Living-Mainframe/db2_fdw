#include <postgres.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/*+ external prototypes */

/** local prototypes */
void* db2alloc         (const char* type, size_t size);
void* db2realloc       (void* p, size_t size);
void  db2free          (void* p);
char* db2strdup        (const char* source);

/* db2alloc
 * Expose palloc0() to DB2 functions.
 */
void* db2alloc (const char* type, size_t size) {
  void* memory = palloc0(size);
  db2Debug5("++ %x: %d bytes - %s", memory, size, type);
  return memory;
}

/* db2realloc
 * Expose repalloc() to DB2 functions.
 */
void* db2realloc (void* p, size_t size) {
  void* memory = repalloc(p, size);
  db2Debug5("++ %x: %d bytes", memory, size);
  return memory;
}

/* db2free
 * Expose pfree() to DB2 functions.
 */
void db2free (void* p) {
  if (p != NULL) {
    db2Debug5("-- %x", p);
    pfree (p);
  }
}

/* db2strdup
 * Expose pstrdup() to DB2 functions.
 */
char* db2strdup(const char* source) {
  char* target = NULL;
  if (source != NULL && source[0] != '\0') {
    target = pstrdup(source);
  }
  db2Debug5("++ %x: dup'ed string from %x content '%s'",target, source, source);
  return target;
}