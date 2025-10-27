#ifndef DB2FDWOPTION_H
#define DB2FDWOPTION_H

/** DB2FdwOption
 *  Describes the valid options for objects that use this wrapper.
 * 
 *  @author Ing Wolfgang Brandl
 *  @since  1.0
 */
typedef struct db2FdwOption {
  const char*         optname;      // option name
  Oid                 optcontext;   // Oid of catalog in which option may appear
  bool                optrequired;  // TRUE / FALSE if option is mandatory
} DB2FdwOption;
#define option_count (sizeof(valid_options)/sizeof(DB2FdwOption))
#endif