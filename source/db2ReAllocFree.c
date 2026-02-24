#include <postgres.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/*+ external prototypes */
extern void db2Entry  (int level, const char* message, ...);
extern void db2Exit   (int level, const char* message, ...);
extern void db2Debug  (int level, const char* message, ...);

/** local prototypes */
void* db2alloc         (const char* type, size_t size);
void* db2realloc       (void* p, size_t size);
void  db2free          (void* p);
char* db2strdup        (const char* source);

/** db2alloc
 *   Expose palloc() to DB2 functions.
 */
void* db2alloc (const char* type, size_t size) {
  void* memory = palloc0(size);
  db2Entry(5,"db2ReAllocFree.c::db2alloc");
  db2Debug(4,"++ %x: %d bytes - %s", memory, size, type);
  db2Exit (5,"db2ReAllocFree.c::db2alloc");
  return memory;
}

/** db2realloc
 *   Expose repalloc() to DB2 functions.
 */
void* db2realloc (void* p, size_t size) {
  void* memory = repalloc(p, size);
  db2Entry(5,"db2ReAllocFree.c::db2realloc");
  db2Debug(4,"++ %x: %d bytes", memory, size);
  db2Exit(5,"db2ReAllocFree.c::db2realloc");
  return memory;
}

/** db2free
 *   Expose pfree() to DB2 functions.
 */
void db2free (void* p) {
  db2Entry(5,"db2ReAllocFree.c::db2free");
  if (p != NULL) {
    db2Debug(4,"-- %x", p);
    pfree (p);
  }
  db2Exit(5,"db2ReAllocFree.c::db2free");
}

char* db2strdup(const char* source) {
  char* target = NULL;
  db2Entry(5,"db2ReAllocFree.c::db2strdup");
  if (source != NULL && source[0] != '\0') {
    target = pstrdup(source);
  }
  db2Debug(4,"++ %x: dup'ed string from %x content '%s'",target, source, source);
  db2Exit(5,"db2ReAllocFree.c::db2strdup");
  return target;
}