/****************************************************************************
* Copyright (c)2012, NuoDB, Inc.
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
*   * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*   * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*   * Neither the name of NuoDB, Inc. nor the names of its contributors may
*       be used to endorse or promote products derived from this software
*       without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
* OR PROFITS; OR BUSINESS INTERRUPTION)HOWEVER CAUSED AND ON ANY THEORY OF
* LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
* OR OTHERWISE)ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
* ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
****************************************************************************/

/*
 * NuoDB Adapter
 */

#include <ruby.h>
#include "atomic.h"
#include <assert.h>
#include <time.h>
#include <stdio.h>
#include <typeinfo>
#include <stdarg.h>

#define HAVE_CXA_DEMANGLE

#ifdef HAVE_CXA_DEMANGLE
#include <cxxabi.h>
const char * demangle(const char * name)
{
    char buf[1024];
    size_t size = 1024;
    int status;
    char * res = abi::__cxa_demangle (name,
        buf, &size, &status);
    return res;
}
#else
const char* demangle(const char* name)
{
  return name;
}
#endif

extern "C" struct timeval rb_time_timeval(VALUE time);

#define AS_QBOOL(value)((value)? Qtrue : Qfalse)

//------------------------------------------------------------------------------

#include "Connection.h"
#include "BigDecimal.h"
#include "Blob.h"
#include "Bytes.h"
#include "CallableStatement.h"
#include "Clob.h"
#include "DatabaseMetaData.h"
#include "DateClass.h"
#include "ParameterMetaData.h"
#include "PreparedStatement.h"
#include "Properties.h"
#include "ResultList.h"
#include "ResultSet.h"
#include "ResultSetMetaData.h"
#include "SQLException.h"
#include "Savepoint.h"
#include "Statement.h"
#include "TimeClass.h"
#include "Timestamp.h"
#include "SqlTimestamp.h"
#include "SqlDate.h"
#include "SqlTime.h"

// ----------------------------------------------------------------------------
// M O D U L E S

static VALUE m_nuodb;

// ----------------------------------------------------------------------------
// C O N S T A N T S

static VALUE c_nuodb_error;

// ----------------------------------------------------------------------------
// C L A S S E S

static VALUE nuodb_connection_klass;
static VALUE nuodb_statement_klass;
static VALUE nuodb_prepared_statement_klass;
static VALUE nuodb_result_klass;

// ----------------------------------------------------------------------------
// S Y M B O L S

static VALUE sym_database, sym_username, sym_password, sym_schema, sym_timezone;

// ----------------------------------------------------------------------------
// B E H A V I O R S

static const bool ENABLE_CLOSE_HOOK = true;

// ----------------------------------------------------------------------------
// L O G G I N G   A N D   T R A C I N G

#define ENABLE_LOGGING

enum LogLevel
{
    ERROR,
    WARN,
    INFO,
    TRACE,
    DEBUG,
    NONE
};

static LogLevel logLevel = NONE;

static char const * log_level_name(LogLevel level)
{
    char const * level_name = NULL;
    switch(level)
    {
    case NONE:
        level_name = "NONE";
        break;
    case ERROR:
        level_name = "ERROR";
        break;
    case WARN:
        level_name = "WARN";
        break;
    case INFO:
        level_name = "INFO";
        break;
    case DEBUG:
        level_name = "DEBUG";
        break;
    case TRACE:
        level_name = "TRACE";
        break;
    }
    return level_name;
}

static void log(LogLevel level, char const * message)
{
    if (level >= logLevel)
    {
        printf("[%s] %s\n", log_level_name(level), message);
    }
}

static void trace(char const * message)
{
    log(TRACE, message);
}

static void print_address(char const * context, void * address)
{
    if (DEBUG >= logLevel)
    {
	    unsigned char *p = (unsigned char *)&address;
	    int i;
		printf("%s: ", context);
	    for (i = 0; i < sizeof address; i++)
	    {
	        printf("%02x ", p[i]);
	    }
	    putchar('\n');
    }
}

// ----------------------------------------------------------------------------
// NEXT GEN GC INTEGRATION SERVICES

#ifdef ENABLE_NEXTGEN_GC_INTEGRATION

/*
 * GC Notes:
 *
 * 1. gc_entities are placed in the Ruby object table and are freed via mark
 *    and sweep.
 * 2. gc_entities may be freed in any order.
 * 3. gc_entities may be freed either before shutdown or during shutdown.
 * 4. gc_entities may no longer be accessed once freed by the garbage
 *    collector; once gc_entities are freed they no longer appear in the
 *    object table. As such, calls to Data_Get_Struct on previously garbage
 *    collected entities will cause the Ruby VM to crash; viz. SIGABRT via
 *    EXC_BAD_ACCESS.
 *
 *    a. So a general observation of this is thusly: if e.g. with graphs of
 *       child to parent relationships an orderly de-allocation is required,
 *       as parents (gc_entities) may be freed prior to childrens (gc_entities)
 *       it becomes imperative that the logic between freeing gc_entity objects
 *       be kept completely and distinctly apart from the logic of the managed
 *       entities themselves.
 */

typedef void (*callback)(void *);

/*
 * Application Rules:
 *
 * 1. Assign a callback for the increment and decrement refers counter so that
 *    context specific logging and tracking may be implemented.
 * 2. Direct access to refers should never occur.
 * 3. The freed bit indicates that the resources associated to the data_handle
 *    has been freed, and NOT that the os_entity struct has been freed.
 *
 * Logic:
 *
 * 1. The incr_func is called on a child when it is first created.
 * 2. The incr_func is called on a parent when its child is first created.
 * 3. When incr_func is called, ATOMIC_INC increments the refers field.
 * 4. When decr_func is called, ATOMIC_DEC decrements the refers field.
 * 5. When the refers field reaches zero (inside decr_func):
 *    5.1. ATOMIC-CAS the freed bit, if the prior freed bit is not set:
 *       5.1.1. ATOMIC-OR all parents freed bits and if the result does not have
 *          the freed bit set:
 *          5.1.1.1. Call the free function, passing the os_entity itself as a parameter.
 *            The free function releases any resources related to the data
 *            handle itself and sets the handle to null.
 *    5.2. Call the decr_func on its parent (this may act recursively).
 *    5.3. Call xfree on the os_entity structure itself.
 * 6. When a user calls finish on an object:
 *    6.1. ATOMIC-CAS the freed bit, if the prior freed bit is not set
 *         perform operations 5.1.1 through 5.1.1.1.
 *
 * Details:
 *
 * 1. Regarding 5a, above, we neither want to doubly release an object, nor
 *    call free_function when a parent has already released all the resources.
 *    This optionally is configurable, for cases when parents forcibly close
 *    child resources, or for cases where they don't. This is a behavioral
 *    configuration parameter to the resource manager. But the above documentation
 *    is tailored for cases where parents automatically release child resources
 *    when they themselves are closed.
 * 2. A race condition may occur between 5.1 and 5.2, above, where a parent
 *    resource is released just after 5.1.1.1 and before 5.2; the result will
 *    be a SEGV. This may prompt the use of a global lock for the reference
 *    manager.
 *
 * Interface:
 *
 * 1. Structures as defined below.
 * 2. Function to release a reference:
 *      void rc_decr_refs (void * handle, int * err_code)
 * 3. Function to acquire a reference:
 *      void rc_incr_refs (void * handle, int * err_code)
 * 4. Function to wrap an entity:
 *      void * rc_add_object (void * object, int * err_code)
 * 5. Function to get wrapped pointer:
 *      void * rc_get_object (void * handle, int * err_code)
 *
 * Questions:
 *
 * 1. Do we go opaque so that the data type definition is malleable? We don't
 *    want this any more type specific than it is.
 */
struct os_entity
{
    void * data_handle;
    unsigned int flags;
    rb_atomic_t refers;

    callback incr_func;
    callback decr_func;
    callback free_func;

    os_entity * parent;
};

/*
 * Rules:
 *
 * 1. When mark is called, traverse marks list till a null is reached, for
 *    each value call rb_gc_mark.
 * 2. When the garbage collector frees an object, the decr_function is called
 *    on the entity passing the entity as its parameter.
 */
struct gc_entity
{
    os_entity * entity;
    VALUE ** mark_refs;
};

/*
 * A strict release strategy only permits free calls after the reference count
 * drops to zero. A lenient release strategy permits free calls at any time,
 * yet the entity itself is not freed until its reference count drops to zero.
 */
typedef enum {strict, lenient} free_strategy;

#endif /* ENABLE_NEXTGEN_GC_INTEGRATION */

// ----------------------------------------------------------------------------
// H A N D L E S

struct nuodb_handle
{
    RUBY_DATA_FUNC free_func;
    rb_atomic_t atomic;
    nuodb_handle * parent_handle;
    VALUE parent;
};

struct nuodb_connection_handle : nuodb_handle
{
    VALUE database;
    VALUE username;
    VALUE password;
    VALUE schema;
    VALUE timezone;

    NuoDB::Connection * pointer;
};

struct nuodb_prepared_statement_handle : nuodb_handle
{
    NuoDB::PreparedStatement * pointer;
};

struct nuodb_statement_handle : nuodb_handle
{
    NuoDB::Statement * pointer;
};

struct nuodb_result_handle : nuodb_handle
{
    NuoDB::ResultSet * pointer;
    NuoDB::Connection * connection;
};

template<typename handle_type>
handle_type * cast_handle(VALUE value)
{
    Check_Type(value, T_DATA);
    return static_cast<handle_type*>(DATA_PTR(value));
}

template<typename handle_type, typename return_type>
return_type * cast_pointer_member(VALUE value)
{
    Check_Type(value, T_DATA);
    return cast_handle<handle_type>(value)->pointer;
}

static void track_ref_count(char const * context, nuodb_handle * handle)
{
    trace("track_ref_count");

    if (handle != 0)
    {
        int parent_count = -10;
        if (handle->parent_handle != 0)
        {
            parent_count = handle->parent_handle->atomic;
        }
        if (logLevel <= DEBUG)
        {
            nuodb_handle  * parent = handle->parent_handle;
		    unsigned char *p = (unsigned char *)&handle;
		    unsigned char *q = (unsigned char *)&parent;
		    int i;
		    printf("[REFERENCE COUNT][%s] (%s @ ", context, demangle(typeid(*handle).name()));
		    for (i = 0; i < sizeof handle; i++)
		    {
		        printf("%02x ", p[i]);
		    }
		    printf("): %d (%s @ ", handle->atomic, demangle(typeid(*parent).name()));
		    for (i = 0; i < sizeof parent; i++)
		    {
		        printf("%02x ", q[i]);
		    }
		    printf("): %d", parent_count);
		    putchar('\n');
        }
    }
}

void incr_reference_count(nuodb_handle * handle)
{
    trace("incr_reference_count");

    track_ref_count("I INCR", handle);

    ATOMIC_INC(handle->atomic);
    if (handle->parent_handle != 0)
    {
        log(DEBUG, "incrementing parent");
        ATOMIC_INC(handle->parent_handle->atomic);
    }

    track_ref_count("O INCR", handle);
}

void decr_reference_count(nuodb_handle * handle)
{
    trace("decr_reference_count");

    track_ref_count("I DECR", handle);

    if (handle->atomic == 0)
    {
        return;
    }

    if (ATOMIC_DEC(handle->atomic) == 0)
    {
        (*(handle->free_func))(handle);
        if (handle->parent_handle != 0)
        {
            log(DEBUG, "decrementing parent");
            decr_reference_count(handle->parent_handle);
            handle->parent = Qnil;
            handle->parent_handle = 0;
            assert(NIL_P(handle->parent));
        }
        track_ref_count("O DECR", handle);

        print_address("[DELETING HANDLE]", handle);
        xfree(handle);
        handle = NULL;
    }
    else
    {
        track_ref_count("O DECR", handle);
    }
}

//------------------------------------------------------------------------------
// exception mapper

static ID c_error_code_assignment;

static void rb_raise_nuodb_error(int code, const char * fmt, ...)
{
    va_list args;
    char text[BUFSIZ];

    va_start(args, fmt);
    vsnprintf(text, BUFSIZ, fmt, args);
    va_end(args);

    VALUE error = rb_exc_new2(c_nuodb_error, text);
    rb_funcall(error, c_error_code_assignment, 1, UINT2NUM(code));
    rb_exc_raise(error);
}

//------------------------------------------------------------------------------

using namespace NuoDB;

//------------------------------------------------------------------------------

static
VALUE nuodb_result_free_protect(VALUE value)
{
    trace("nuodb_result_free_protect");
    nuodb_result_handle * handle = reinterpret_cast<nuodb_result_handle *>(value);
    if (handle != NULL)
    {
        if (handle->pointer != NULL)
        {
            try
            {
                track_ref_count("CLOSE RESULT", handle);
                log(INFO, "closing result");
                handle->pointer->close();
                handle->pointer = NULL;
            }
            catch (SQLException & e)
            {
                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close result: %s", e.getText());
            }
        }
    }
    return Qnil;
}

static
void nuodb_result_free(void * ptr)
{
    trace("nuodb_result_free");
    if (ptr != NULL)
    {
        int exception = 0;
        rb_protect(nuodb_result_free_protect, reinterpret_cast<VALUE>(ptr), &exception);
        if (exception)
        {
            rb_jump_tag(exception);
        }
    }
}

static
void nuodb_result_mark(void * ptr)
{
    trace("nuodb_result_mark");
    nuodb_result_handle * handle = static_cast<nuodb_result_handle *>(ptr);
    rb_gc_mark(handle->parent);
}

static
void nuodb_result_decr_reference_count(nuodb_handle * handle)
{
    trace("nuodb_result_decr_reference_count");
    decr_reference_count(handle);
}

/*
 * call-seq:
 *  finish()
 *
 * Releases the result set and any associated resources.
 */
static
VALUE nuodb_result_finish(VALUE self)
{
    trace("nuodb_result_free_protect");
    nuodb_result_handle * handle = cast_handle<nuodb_result_handle>(self);//reinterpret_cast<nuodb_result_handle *>(value);
    nuodb_result_free(handle);
//    if (handle != NULL)
//    {
//        if (handle->pointer != NULL)
//        {
//            try
//            {
//                //handle->pointer->close();
//                //handle->pointer = NULL;
//            }
//            catch (SQLException & e)
//            {
//                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close result: %s", e.getText());
//            }
//        }
//    }
    return Qnil;
//
//
//
//    trace("nuodb_result_finish");
//    if (ENABLE_CLOSE_HOOK)
//    {
//        //nuodb_result_decr_reference_count(cast_handle<nuodb_result_handle>(self));
//    }
//    nuodb_result_handle * handle = ;
//    if (handle != NULL && handle->pointer != NULL)
//    {
//        if (ATOMIC_DEC(handle->atomic) == 0)
//        {
//            try
//            {
//                //handle->pointer->close();
//                //handle->pointer = NULL;
//            }
//            catch (SQLException & e)
//            {
//                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close result set: %s", e.getText());
//            }
//        }
//    }
//    return Qnil;
}

static
VALUE nuodb_map_sql_type(int type)
{
    VALUE symbol = Qnil;
    switch(type)
    {
    case NUOSQL_TINYINT:
    case NUOSQL_SMALLINT:
    case NUOSQL_INTEGER:
    case NUOSQL_BIGINT:
        symbol = ID2SYM(rb_intern("integer"));
        break;
    
    case NUOSQL_BINARY:
	symbol = ID2SYM(rb_intern("binary"));
	break;
    
    case NUOSQL_FLOAT:
    case NUOSQL_DOUBLE:
        symbol = ID2SYM(rb_intern("float"));
        break;

    case NUOSQL_CHAR:
    case NUOSQL_VARCHAR:
    case NUOSQL_LONGVARCHAR:
        symbol = ID2SYM(rb_intern("string"));
        break;

    case NUOSQL_BIT:
    case NUOSQL_BOOLEAN:
        symbol = ID2SYM(rb_intern("boolean"));
        break;

    case NUOSQL_DATE:
        symbol = ID2SYM(rb_intern("date"));
        break;

    case NUOSQL_TIMESTAMP:
        symbol = ID2SYM(rb_intern("timestamp"));
        break;

    case NUOSQL_TIME:
        symbol = ID2SYM(rb_intern("time"));
        break;

    case NUOSQL_DECIMAL:
        symbol = ID2SYM(rb_intern("decimal"));
        break;

    case NUOSQL_NUMERIC:
        symbol = ID2SYM(rb_intern("numeric"));
        break;

//    case NUOSQL_BLOB:
//        symbol = ID2SYM(rb_intern("blob"));
//        break;
//
//    case NUOSQL_CLOB:
//        symbol = ID2SYM(rb_intern("clob"));
//        break;

    case NUOSQL_NULL:
    case NUOSQL_BLOB:
    case NUOSQL_CLOB:
    case NUOSQL_LONGVARBINARY:
    default:
        rb_raise(rb_eNotImpError, "Unsupported SQL type: %d", type);
    }
    return symbol;
}
static
VALUE nuodb_result_alloc(VALUE parent, NuoDB::ResultSet * results, NuoDB::Connection * connection)
{
    trace("nuodb_result_alloc");
    nuodb_handle * parent_handle = cast_handle<nuodb_handle>(parent);
    if (parent_handle != NULL)
    {
        nuodb_result_handle * handle = ALLOC(struct nuodb_result_handle);
        handle->free_func = RUBY_DATA_FUNC(nuodb_result_free);
        handle->atomic = 0;
        handle->parent = parent;
        handle->parent_handle = parent_handle;
        handle->pointer = results;
        handle->connection = connection;
        incr_reference_count(handle);
        VALUE self = Data_Wrap_Struct(nuodb_result_klass, nuodb_result_mark, nuodb_result_decr_reference_count, handle);

        rb_iv_set(self, "@columns", Qnil);
        rb_iv_set(self, "@rows", Qnil);

        if (!rb_block_given_p()) {
            trace("nuodb_result_alloc: no block");

            return self;
        }

        trace("nuodb_result_alloc: begin block");

        int exception = 0;
        VALUE result = rb_protect(rb_yield, self, &exception);

        trace("nuodb_result_alloc: end block");

        //nuodb_result_finish(self);

        trace("nuodb_result_alloc: auto finish");

        if (exception)
        {
            rb_jump_tag(exception);
        }
        else
        {
            return result;
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: statement handle nil");
    }
    return Qnil;
}

static int64_t
nuodb_get_rb_timezone_offset()
{
    VALUE time = rb_funcall(rb_cTime, rb_intern("at"), 1, rb_float_new(0));
    VALUE offset = rb_funcall(time, rb_intern("utc_offset"), 0);
    return NUM2LONG(offset);
}

static VALUE
nuodb_get_rb_value(int column, SqlType type, ResultSet * results)
{
    VALUE value = Qnil;
    switch (type)
    {
        case NUOSQL_BIT:
        case NUOSQL_BOOLEAN:
        {
            // try-catch b.c. http://tools/jira/browse/DB-2379
            try
            {
                bool field = results->getBoolean(column);
                if (!results->wasNull())
                {
                    value = AS_QBOOL(field);
                }
            }
            catch (SQLException & e)
            {
                 // see JDBC spec, DB-2379, however, according to RoR rules this
                 // should return nil. See the following test case:
                 // test_default_values_on_empty_strings(BasicsTest) [test/cases/base_test.rb:]
            }
            break;
        }
        case NUOSQL_FLOAT:
        case NUOSQL_DOUBLE:
        {
            double field = results->getDouble(column);
            if (!results->wasNull())
            {
                value = rb_float_new(field);
            }
            break;
        }
        case NUOSQL_TINYINT:
        case NUOSQL_SMALLINT:
        case NUOSQL_INTEGER:
        {
            int field = results->getInt(column);
            if (!results->wasNull())
            {
                value = INT2NUM(field);
            }
            break;
        }
        case NUOSQL_BIGINT:
        {
            int64_t field = results->getLong(column);
            if (!results->wasNull())
            {
                value = LONG2NUM(field);
            }
            break;
        }
	case NUOSQL_BINARY:
        case NUOSQL_VARCHAR:
        case NUOSQL_LONGVARCHAR:
        {
            char const * field = results->getString(column);
            if (!results->wasNull())
            {
                value = rb_str_new2(field);
            }
            break;
        }
        case NUOSQL_DATE:
        {
            NuoDB::Date * field = results->getDate(column);
            if (!results->wasNull())
            {
                double secs = (double) field->getSeconds();
                VALUE time = rb_funcall(rb_cTime, rb_intern("at"), 1, rb_float_new(secs - nuodb_get_rb_timezone_offset()));
                value = rb_funcall(time, rb_intern("to_date"), 0);
            }
            break;
        }
        case NUOSQL_TIME:
        case NUOSQL_TIMESTAMP:
        {
            NuoDB::Timestamp * field = results->getTimestamp(column);
            if (!results->wasNull())
            {
                double secs = ((double) field->getSeconds()) + (((double) field->getNanos()) / 1000000);
                value = rb_funcall(rb_cTime, rb_intern("at"), 1, rb_float_new(secs)); //  - nuodb_get_rb_timezone_offset()
            }
            break;
        }
        case NUOSQL_NUMERIC:
        {
            char const * field = results->getString(column);
            if (!results->wasNull())
            {
                rb_require("bigdecimal");
                VALUE klass = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
                VALUE args[1];
                args[0] = rb_str_new2(field);
                value = rb_class_new_instance(1, args, klass);
            }
            break;
        }
        default:
        {
            rb_raise(rb_eTypeError, "CHANGES WORKING: %d", type);
            break;
        }
    }
    return value;
}

/*
 * call-seq:
 *      result.columns -> ary
 *
 * Returns an array of Column objects.
 *
 *      results = statement.results
 *      ...
 *      results.columns.each do |column|
 *          puts "#{column.name}, #{column.default}, #{column.type}, #{column.null}"
 *      end
 */
static VALUE
nuodb_result_columns(VALUE self)
{
    trace("nuodb_result_columns");
    nuodb_result_handle * handle = cast_handle<nuodb_result_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        VALUE columns = rb_iv_get(self, "@columns");
        if (NIL_P(columns))
        {
            try
            {
                ResultSetMetaData * result_metadata = handle->pointer->getMetaData();
                DatabaseMetaData * database_metadata = handle->connection->getMetaData();

                VALUE array = rb_ary_new();
                rb_require("nuodb/column");
                VALUE column_klass = rb_const_get(m_nuodb, rb_intern("Column"));

                int column_count = result_metadata->getColumnCount();
                for (int column_index = 1; column_index < column_count + 1; ++column_index)
                {
                    char const * schema_name = result_metadata->getSchemaName(column_index);
                    char const * table_name = result_metadata->getTableName(column_index);
                    char const * column_name = result_metadata->getColumnName(column_index);

                    // args: [name, default, sql_type, null]
                    VALUE args[4];

                    args[0] = rb_str_new2(result_metadata->getColumnLabel(column_index));

                    ResultSet * database_metadata_results = database_metadata->getColumns(NULL,
                        schema_name, table_name, column_name);

                    if(database_metadata_results->next())
                    {
                        try
                        {
                            args[1] = nuodb_get_rb_value(database_metadata_results->findColumn("COLUMN_DEF"),
                                (SqlType) result_metadata->getColumnType(column_index), database_metadata_results);
                        }
                        catch (SQLException & e)
                        {
                            args[1] = Qnil;
                        }
                    }
                    else
                    {
                        args[1] = Qnil;
                    }
                    database_metadata_results->close();

                    args[2] = nuodb_map_sql_type(result_metadata->getColumnType(column_index));
                    args[3] = result_metadata->isNullable(column_index) ? Qtrue : Qfalse;

                    VALUE column = rb_class_new_instance(4, args, column_klass);
                    rb_ary_push(array, column);
                }

                rb_iv_set(self, "@columns", array);

                return array;
            }
            catch (SQLException & e)
            {
                rb_raise_nuodb_error(e.getSqlcode(), "Failed to create column info: %s", e.getText());
            }
        }
        else
        {
            return columns;
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: result handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *      result.rows -> ary
 *
 * Returns an array of rows, each of which is an array of values.
 *
 * Note that calling #rows for large result sets is sub-optimal as it will load
 * the entire dataset into memory. Users should prefer calling #each over #rows.
 *
 *      results = statement.results
 *      ...
 *      results.rows.each do |row|
 *          row.each do |value|
 *              puts value
 *          end
 *      end
 */
static VALUE
nuodb_result_rows(VALUE self)
{
    trace("nuodb_result_rows");
    nuodb_result_handle * handle = cast_handle<nuodb_result_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        VALUE rows = rb_iv_get(self, "@rows");
        if (NIL_P(rows))
        {
            try
            {
                rows = rb_ary_new();

                while (handle->pointer->next())
                {
                    VALUE row = rb_ary_new();

                    NuoDB::ResultSetMetaData * metadata = handle->pointer->getMetaData();
                    int32_t column_count = metadata->getColumnCount();
                    for (int32_t column = 1; column < column_count + 1; column++)
                    {
                        SqlType type = (SqlType) metadata->getColumnType(column);
                        rb_ary_push(row, nuodb_get_rb_value(column, type, handle->pointer));
                    }

                    rb_ary_push(rows, row);
                }

                rb_iv_set(self, "@rows", rows);
            }
            catch (SQLException & e)
            {
                rb_raise_nuodb_error(e.getSqlcode(), "Failed to create a rows array: %s", e.getText());
            }
        }
        return rows;
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: result handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *      result.each { |tuple| ... }
 *
 * Invokes the block for each tuple in the result set.
 *
 *      connection.prepare select_dml do |select|
 *          ...
 *          if select.execute
 *              select.results.each do |row|
 *                  ...
 *              end
 *          end
 *      end
 */
static VALUE
nuodb_result_each(VALUE self)
{
    trace("nuodb_result_each");
    nuodb_result_handle * handle = cast_handle<nuodb_result_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        VALUE rows = rb_iv_get(self, "@rows");
        if (NIL_P(rows))
        {
			rows = nuodb_result_rows(self);
        }
        for (int i = 0; i < RARRAY_LEN(rows); i++)
        {
            rb_yield(rb_ary_entry(rows, i));
        }
        return self;
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: result handle nil");
    }
    return Qnil;
}

static
void nuodb_define_result_api()
{
    /*
     * A Result object maintains a connection to a specific database. SQL
     * statements are executed and results are returned within the context of
     * a connection.
     */
    nuodb_result_klass = rb_define_class_under(m_nuodb, "Result", rb_cObject);
    rb_include_module(nuodb_result_klass, rb_mEnumerable);

    rb_define_attr(nuodb_result_klass, "columns", 1, 0);
    rb_define_attr(nuodb_result_klass, "rows", 1, 0);

    // DBI

    rb_define_method(nuodb_result_klass, "each", RUBY_METHOD_FUNC(nuodb_result_each), 0);
    rb_define_method(nuodb_result_klass, "columns", RUBY_METHOD_FUNC(nuodb_result_columns), 0);
    rb_define_method(nuodb_result_klass, "rows", RUBY_METHOD_FUNC(nuodb_result_rows), 0);
    //rb_define_method(nuodb_result_klass, "finish", RUBY_METHOD_FUNC(nuodb_result_finish), 0);
}

//------------------------------------------------------------------------------

static
VALUE nuodb_statement_free_protect(VALUE value)
{
    trace("nuodb_statement_free_protect");
    nuodb_statement_handle * handle = reinterpret_cast<nuodb_statement_handle*>(value);
    if (handle != NULL)
    {
        if (handle->pointer != NULL)
        {
            try
            {
                log(INFO, "closing statement");
                handle->pointer->close();
                handle->pointer = NULL;
            }
            catch (SQLException & e)
            {
                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close statement: %s", e.getText());
            }
        }
    }
    return Qnil;
}

static
void nuodb_statement_free(void * ptr)
{
    trace("nuodb_statement_free");
    if (ptr != NULL)
    {
        int exception = 0;
        rb_protect(nuodb_statement_free_protect, reinterpret_cast<VALUE>(ptr), &exception);
        if (exception)
        {
            rb_jump_tag(exception);
        }
    }
}

static
void nuodb_statement_mark(void * ptr)
{
    trace("nuodb_statement_mark");

    nuodb_statement_handle * handle = static_cast<nuodb_statement_handle *>(ptr);
    rb_gc_mark(handle->parent);
}

static
void nuodb_statement_decr_reference_count(void * ptr)
{
    trace("nuodb_statement_decr_reference_count");
    decr_reference_count(static_cast<nuodb_statement_handle *>(ptr));
}

/*
 * call-seq:
 *  finish()
 *
 * Releases the statement and any associated resources.
 */
static
VALUE nuodb_statement_finish(VALUE self)
{
    trace("nuodb_statement_finish");
    nuodb_statement_handle * handle = cast_handle<nuodb_statement_handle>(self);//reinterpret_cast<nuodb_statement_handle*>(value);
    nuodb_statement_free(handle);
//    if (handle != NULL)
//    {
//        if (handle->pointer != NULL)
//        {
//            try
//            {
//                log(INFO, "closing statement");
//                handle->pointer->close();
//                handle->pointer = NULL;
//            }
//            catch (SQLException & e)
//            {
//                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close statement: %s", e.getText());
//            }
//        }
//    }
    return Qnil;
//
//    if (ENABLE_CLOSE_HOOK)
//    {
//        nuodb_statement_handle * handle = cast_handle<nuodb_statement_handle>(self);
//        if (handle != NULL && handle->pointer != NULL)
//        {
//            nuodb_statement_decr_reference_count(handle);
//        }
//        else
//        {
//            rb_raise(rb_eArgError, "invalid state: statement handle nil");
//        }
//    }
//    return Qnil;
}

static
VALUE nuodb_statement_initialize(VALUE parent)
{
    trace("nuodb_statement_initialize");

    nuodb_connection_handle * parent_handle = cast_handle<nuodb_connection_handle>(parent);
    if (parent_handle != NULL && parent_handle->pointer != NULL)
    {
        NuoDB::Statement * statement = NULL;
        try
        {
            statement = parent_handle->pointer->createStatement();
        }
        catch (SQLException & e)
        {
            log(ERROR, "rb_raise");
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to create statement: %s", e.getText());
        }

        nuodb_statement_handle * handle = ALLOC(nuodb_statement_handle);

        handle->free_func = RUBY_DATA_FUNC(&nuodb_statement_free);
        handle->atomic = 0;
        handle->parent = parent;
        handle->parent_handle = parent_handle;
        handle->pointer = statement;
        incr_reference_count(handle);
        assert(handle->atomic = 1);
        VALUE self = Data_Wrap_Struct(nuodb_statement_klass, nuodb_statement_mark, nuodb_statement_decr_reference_count, handle);
        if (!rb_block_given_p()) {
            trace("nuodb_statement_initialize: no block");
            track_ref_count("ALLOC STMT S", cast_handle<nuodb_handle>(self));
            return self;
        }

        trace("nuodb_statement_initialize: begin block");

        int exception = 0;
        VALUE result = rb_protect(rb_yield, self, &exception);

        trace("nuodb_statement_initialize: end block");

        // n.b. don't do this as it may introduce crashes of the ruby process !!!
        //nuodb_statement_finish(self);

        trace("nuodb_statement_initialize: auto finish");

        if (exception)
        {
            rb_jump_tag(exception);
        }
        else
        {
            return result;
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  execute(sql) -> Bool
 *
 * Executes a statement.
 *
 * Returns true if the result is a Result object; returns false if the result is
 * an update count or there is no result. For the latter case, count() should be
 * called next, for the former case each() should be called.
 */
static
VALUE nuodb_statement_execute(VALUE self, VALUE sql)
{
    if (TYPE(sql) != T_STRING)
    {
        rb_raise(rb_eTypeError, "wrong sql argument type %s (String expected)", rb_class2name(CLASS_OF(sql)));
    }

    nuodb_statement_handle * handle = cast_handle<nuodb_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            return AS_QBOOL(handle->pointer->execute(StringValueCStr(sql),
                NuoDB::RETURN_GENERATED_KEYS));
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  count() -> Number
 *
 * Returns the update count for an update statement.
 */
static
VALUE nuodb_statement_update_count(VALUE self)
{
    trace("nuodb_statement_update_count");

    nuodb_statement_handle * handle = cast_handle<nuodb_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            return INT2NUM(handle->pointer->getUpdateCount());
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the update count for the statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  results() -> Results
 *
 * Retrieves a result set containing rows for the related query.
 */
static VALUE nuodb_statement_results(VALUE self)
{
    trace("nuodb_statement_results");

    nuodb_statement_handle * handle = cast_handle<nuodb_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            return nuodb_result_alloc(self, handle->pointer->getResultSet(), handle->pointer->getConnection());
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the result set for the statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  generated_keys() -> Results
 *
 * Retrieves a result set containing the generated keys related to the
 * previously executed insert.
 */
static VALUE nuodb_statement_generated_keys(VALUE self)
{
    trace("nuodb_statement_generated_keys");

    nuodb_statement_handle * handle = cast_handle<nuodb_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            // this hack should not have been necessary; it should never have
            // returned null, this is a product defect.
            ResultSet * results = handle->pointer->getGeneratedKeys();
            if (results != NULL)
            {
                return nuodb_result_alloc(self, results, handle->pointer->getConnection());
            }
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the generated keys for the statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: statement handle nil");
    }
    return Qnil;
}

static
void nuodb_define_statement_api()
{
    /*
     * Document-class: NuoDB::Statement
     *
     * A Statement object maintains a connection to a specific database. SQL
     * statements are executed and results are returned within the context of
     * a connection.
     */
    nuodb_statement_klass = rb_define_class_under(m_nuodb, "Statement", rb_cObject);

    // DBI

    rb_define_method(nuodb_statement_klass, "execute", RUBY_METHOD_FUNC(nuodb_statement_execute), 1);
    //rb_define_method(nuodb_statement_klass, "finish", RUBY_METHOD_FUNC(nuodb_statement_finish), 0);

    // NUODB EXTENSIONS

    rb_define_method(nuodb_statement_klass, "count", RUBY_METHOD_FUNC(nuodb_statement_update_count), 0);
    rb_define_method(nuodb_statement_klass, "generated_keys", RUBY_METHOD_FUNC(nuodb_statement_generated_keys), 0);
    rb_define_method(nuodb_statement_klass, "results", RUBY_METHOD_FUNC(nuodb_statement_results), 0);
}

//------------------------------------------------------------------------------

static
VALUE nuodb_prepared_statement_free_protect(VALUE value)
{
    trace("nuodb_prepared_statement_free_protect");
    nuodb_prepared_statement_handle * handle = reinterpret_cast<nuodb_prepared_statement_handle *>(value);
    track_ref_count("PS FREE PROTECT", handle);
    if (handle != NULL)
    {
        if (handle->pointer != NULL)
        {
            try
            {
                log(INFO, "closing prepared statement");
                handle->pointer->close();
                handle->pointer = NULL;
            }
            catch (SQLException & e)
            {
                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close statement: %s", e.getText());
            }
        }
    }
    return Qnil;
}

static
void nuodb_prepared_statement_free(void * ptr)
{
    trace("nuodb_prepared_statement_free");

    if (ptr != NULL)
    {
        int exception = 0;
        rb_protect(nuodb_prepared_statement_free_protect, reinterpret_cast<VALUE>(ptr), &exception);
        if (exception)
        {
            rb_jump_tag(exception);
        }
    }
}

static
void nuodb_prepared_statement_mark(void * ptr)
{
    trace("nuodb_prepared_statement_mark");

    nuodb_prepared_statement_handle * handle = static_cast<nuodb_prepared_statement_handle *>(ptr);
    rb_gc_mark(handle->parent);
}

static
void nuodb_prepared_statement_decr_reference_count(nuodb_handle * handle)
{
    trace("nuodb_prepared_statement_decr_reference_count");
    decr_reference_count(handle);
}

/*
 * call-seq:
 *  finish()
 *
 * Releases the prepared statement and any associated resources.
 */
static
VALUE nuodb_prepared_statement_finish(VALUE self)
{
    trace("nuodb_prepared_statement_finish");
    nuodb_prepared_statement_handle * handle = cast_handle<nuodb_prepared_statement_handle>(self);//reinterpret_cast<nuodb_prepared_statement_handle *>(value);
    track_ref_count("FINISH PSTMT", handle);
    nuodb_prepared_statement_free(handle);
//    if (handle != NULL)
//    {
//        if (handle->pointer != NULL)
//        {
//            try
//            {
//                track_ref_count("CLOSE PSTMT", handle);
//                log(INFO, "closing prepared statement");
//                handle->pointer->close();
//                handle->pointer = NULL;
//            }
//            catch (SQLException & e)
//            {
//                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close statement: %s", e.getText());
//            }
//        }
//    }
    return Qnil;
//
//    if (ENABLE_CLOSE_HOOK)
//    {
//        nuodb_prepared_statement_handle * handle = cast_handle<nuodb_prepared_statement_handle>(self);
//        if (handle != NULL && handle->pointer != NULL)
//        {
//            nuodb_prepared_statement_decr_reference_count(handle);
//        }
//        else
//        {
//            rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
//        }
//    }
//    return Qnil;
}

static
VALUE nuodb_prepared_statement_initialize(VALUE parent, VALUE sql)
{
    trace("nuodb_prepared_statement_initialize");

    if (TYPE(sql) != T_STRING)
    {
        rb_raise(rb_eTypeError, "wrong sql argument type %s (String expected)", rb_class2name(CLASS_OF(sql)));
    }

    nuodb_connection_handle * parent_handle = cast_handle<nuodb_connection_handle>(parent);
    if (parent_handle != NULL && parent_handle->pointer != NULL)
    {
        NuoDB::PreparedStatement * statement = NULL;
        try
        {
            statement = parent_handle->pointer->prepareStatement(StringValueCStr(sql),
                    NuoDB::RETURN_GENERATED_KEYS);
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to create prepared statement (%s): %s", sql, e.getText());
        }

        nuodb_prepared_statement_handle * handle = ALLOC(struct nuodb_prepared_statement_handle);
        handle->free_func = RUBY_DATA_FUNC(&nuodb_prepared_statement_free);
        handle->atomic = 0;
        handle->parent = parent;
        handle->parent_handle = parent_handle;
        handle->pointer = statement;
        incr_reference_count(handle);
        assert(handle->atomic == 1);
        VALUE self = Data_Wrap_Struct(nuodb_prepared_statement_klass, nuodb_prepared_statement_mark, nuodb_prepared_statement_decr_reference_count, handle);

        if (!rb_block_given_p()) {
            trace("nuodb_prepared_statement_initialize: no block");

            return self;
        }

        trace("nuodb_prepared_statement_initialize: begin block");

        int exception = 0;
        VALUE result = rb_protect(rb_yield, self, &exception);

        trace("nuodb_prepared_statement_initialize: end block");

        // n.b. don't do this as it may introduce crashes of the ruby process !!!
        //nuodb_prepared_statement_finish(self);

        trace("nuodb_prepared_statement_initialize: auto finish");

        if (exception)
        {
            rb_jump_tag(exception);
        }
        else
        {
            return result;
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

static
void raise_unsupported_type_at_index(char const * type_name, int32_t index)
{
    rb_raise(rb_eTypeError, "unsupported type %s at %d", type_name, index);
}

/*
 * call-seq:
 *  bind_param(param, value)
 *
 * Attempts to bind the value to this statement as the parameter number specified.
 *
 *  connection.prepare insert_dml do |statement|
 *      statement.bind_param(1, "Joe Smith")
 *      statement.execute
 *  end  #=> implicit statement.finish
 */
static
VALUE nuodb_prepared_statement_bind_param(VALUE self, VALUE param, VALUE value)
{
    trace("nuodb_prepared_statement_bind_param");

    if (TYPE(param) != T_FIXNUM)
    {
        rb_raise(rb_eTypeError, "index must be a number");
    }
    int32_t index = NUM2UINT(param);

    NuoDB::PreparedStatement * statement = cast_pointer_member<
                                           nuodb_prepared_statement_handle, NuoDB::PreparedStatement>(self);

    try
    {
        switch (TYPE(value))
        {
        case T_FLOAT: // 0x04
            {
                log(DEBUG, "supported: T_FLOAT");
                double real_value = NUM2DBL(value);
                statement->setDouble(index, real_value);
            }
            break;
        case T_STRING: // 0x05
            {
                log(DEBUG, "supported: T_STRING");
                char const * real_value = RSTRING_PTR(value);
                statement->setString(index, real_value);
            }
            break;
        case T_NIL: // 0x11
            {
                log(DEBUG, "supported: T_NIL");
                statement->setNull(index, 0);
            }
            break;
        case T_TRUE: // 0x12
            {
                log(DEBUG, "supported: T_TRUE");
                statement->setBoolean(index, true);
            }
            break;
        case T_FALSE: // 0x13
            {
                log(DEBUG, "supported: T_FALSE");
                statement->setBoolean(index, false);
            }
            break;
        case T_FIXNUM: // 0x15
            {
                log(DEBUG, "supported: T_FIXNUM");
                int64_t real_value = NUM2LONG(value);
                statement->setLong(index, real_value);
            }
            break;
        case T_DATA: // 0x22
            {
                log(DEBUG, "supported: T_DATA");
                if (rb_obj_is_instance_of(value, rb_cTime))
                {
                    log(DEBUG, "supported Time");
                    VALUE sec = rb_funcall(value, rb_intern("tv_sec"), 0);
                    //VALUE offset = rb_funcall(value, rb_intern("utc_offset"), 0);
                    VALUE usec = rb_funcall(value, rb_intern("tv_usec"), 0);
                    SqlTimestamp sqlTimestamp(NUM2INT(sec), NUM2INT(usec) * 1000); //  + NUM2INT(offset)
                    statement->setTimestamp(index, &sqlTimestamp);
                    break;
                }
                VALUE cDate = rb_const_get(rb_cObject, rb_intern("Date"));
                if (rb_obj_is_instance_of(value, cDate))
                {
                    log(DEBUG, "supported Date");
                    VALUE time = rb_funcall(value, rb_intern("to_time"), 0);
                    VALUE sec = rb_funcall(time, rb_intern("tv_sec"), 0);
                    //VALUE offset = rb_funcall(time, rb_intern("utc_offset"), 0);
                    VALUE usec = rb_funcall(time, rb_intern("tv_usec"), 0);
                    SqlTimestamp sqlTimestamp(NUM2LONG(sec), NUM2INT(usec) * 1000);//  + NUM2INT(offset)
                    statement->setTimestamp(index, &sqlTimestamp);
                    break;
                }
                break;
            }
        case T_OBJECT: // 0x01
            {
                log(WARN, "unsupported: T_OBJECT");
                raise_unsupported_type_at_index("T_OBJECT", index);
            }
            break;
        case T_BIGNUM: // 0x0a
            {
                log(DEBUG, "supported: T_BIGNUM");
                int64_t real_value = NUM2LONG(value);
                statement->setLong(index, real_value);
            }
            break;
        case T_ARRAY: // 0x07
            {
                log(WARN, "unsupported: T_ARRAY");
                raise_unsupported_type_at_index("T_ARRAY", index);
            }
            break;
        case T_HASH: // 0x08
            {
                log(WARN, "unsupported: T_HASH");
                raise_unsupported_type_at_index("T_HASH", index);
            }
            break;
        case T_STRUCT: // 0x09
            {
                log(WARN, "unsupported: T_STRUCT");
                raise_unsupported_type_at_index("T_STRUCT", index);
            }
            break;
        case T_FILE: // 0x0e
            {
                log(WARN, "unsupported: T_FILE");
                raise_unsupported_type_at_index("T_FILE", index);
            }
            break;
        case T_MATCH: // 0x23
            {
                log(WARN, "unsupported: T_MATCH");
                raise_unsupported_type_at_index("T_MATCH", index);
            }
            break;
        case T_SYMBOL: // 0x24
            {
                log(WARN, "unsupported: T_SYMBOL");
                raise_unsupported_type_at_index("T_SYMBOL", index);
                break;
            }
            break;
        default:
            rb_raise(rb_eTypeError, "unsupported type: %d", TYPE(value));
            break;
        }
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to set prepared statement parameter(%d, %lld) failed: %s",
                             index, param, e.getText());
    }
    return Qnil;
}

/*
 * call-seq:
 *  bind_params(*binds)
 *
 * Attempts to bind the list of values successively using bind_param.
 *
 *  connection.prepare insert_dml do |statement|
 *      statement.bind_params([56, 6.7, "String", Date.new(2001, 12, 3), Time.new])
 *      statement.execute
 *  end  #=> implicit statement.finish
 */
static
VALUE nuodb_prepared_statement_bind_params(VALUE self, VALUE array)
{
    trace("nuodb_prepared_statement_bind_params");

    nuodb_prepared_statement_handle * handle = cast_handle<nuodb_prepared_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        for (int i = 0; i < RARRAY_LEN(array); ++i)
        {
            VALUE value = RARRAY_PTR(array)[i];
            nuodb_prepared_statement_bind_param(self, UINT2NUM(i+1), value);
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  execute() -> Bool
 *
 * Executes a prepared statement. Returns true if the result is a Result object;
 * returns false if the result is an update count or there is no result. For the
 * latter case, count() should be called next, for the former case each() should
 * be called.
 */
static
VALUE nuodb_prepared_statement_execute(VALUE self)
{
    trace("nuodb_prepared_statement_execute");

    nuodb_prepared_statement_handle * handle = cast_handle<nuodb_prepared_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            return AS_QBOOL(handle->pointer->execute());
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL prepared statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  count() -> Number
 *
 * Returns the update count for an update statement.
 */
static
VALUE nuodb_prepared_statement_update_count(VALUE self)
{
    trace("nuodb_prepared_statement_update_count");

    nuodb_prepared_statement_handle * handle = cast_handle<nuodb_prepared_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            return INT2NUM(handle->pointer->getUpdateCount());
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the update count for the prepared statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  results() -> Results
 *
 * Retrieves a result set containing rows for the related query.
 */
static VALUE nuodb_prepared_statement_results(VALUE self)
{
    trace("nuodb_prepared_statement_results");

    nuodb_prepared_statement_handle * handle = cast_handle<nuodb_prepared_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            return nuodb_result_alloc(self, handle->pointer->getResultSet(), handle->pointer->getConnection());
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the result set for the prepared statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  generated_keys() -> Results
 *
 * Retrieves a result set containing the generated keys related to the
 * previously executed insert.
 */
static VALUE nuodb_prepared_statement_generated_keys(VALUE self)
{
    trace("nuodb_prepared_statement_generated_keys");

    nuodb_prepared_statement_handle * handle = cast_handle<nuodb_prepared_statement_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            // this hack should not have been necessary; it should never have
            // returned null, this is a product defect.
            ResultSet * results = handle->pointer->getGeneratedKeys();
            if (results != NULL)
            {
                return nuodb_result_alloc(self, results, handle->pointer->getConnection());
            }
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the generated keys for the prepared statement: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: prepared statement handle nil");
    }
    return Qnil;
}

static
void nuodb_define_prepared_statement_api()
{
    /*
     * Document-class: NuoDB::PreparedStatement
     *
     * A PreparedStatement object maintains a connection to a specific database.
     * SQL statements are executed and results are returned within the context of a
     * connection.
     */
    nuodb_prepared_statement_klass = rb_define_class_under(m_nuodb, "PreparedStatement", rb_cObject);

    // DBI

    rb_define_method(nuodb_prepared_statement_klass, "bind_param", RUBY_METHOD_FUNC(nuodb_prepared_statement_bind_param), 2);
    rb_define_method(nuodb_prepared_statement_klass, "bind_params", RUBY_METHOD_FUNC(nuodb_prepared_statement_bind_params), 1);
    rb_define_method(nuodb_prepared_statement_klass, "execute", RUBY_METHOD_FUNC(nuodb_prepared_statement_execute), 0);
    //rb_define_method(nuodb_prepared_statement_klass, "finish", RUBY_METHOD_FUNC(nuodb_prepared_statement_finish), 0);

    // NUODB EXTENSIONS

    rb_define_method(nuodb_prepared_statement_klass, "count", RUBY_METHOD_FUNC(nuodb_prepared_statement_update_count), 0);
    rb_define_method(nuodb_prepared_statement_klass, "generated_keys", RUBY_METHOD_FUNC(nuodb_prepared_statement_generated_keys), 0);
    rb_define_method(nuodb_prepared_statement_klass, "results", RUBY_METHOD_FUNC(nuodb_prepared_statement_results), 0);
}

//------------------------------------------------------------------------------

/*
 * Class NuoDB::Connection
 */

static
VALUE nuodb_connection_free_protect(VALUE value)
{
    trace("nuodb_connection_free_protect");

    nuodb_connection_handle * handle = reinterpret_cast<nuodb_connection_handle *>(value);
    track_ref_count("FREE CONN CHECK", handle);
    if (handle != NULL)
    {
        track_ref_count("FREE CONN", handle);
        if (handle->pointer != NULL)
        {
            try
            {
                track_ref_count("CLOSE CONN", handle);
                log(INFO, "closing connection");
                handle->pointer->close();
                handle->pointer = NULL;
            }
            catch (SQLException & e)
            {
                log(DEBUG, "rb_raise");
                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close connection: %s", e.getText());
            }
        }
    }
    return Qnil;
}

static
void nuodb_connection_free(void * ptr)
{
    trace("nuodb_connection_free");
    if (ptr != NULL)
    {
        int exception = 0;
        rb_protect(nuodb_connection_free_protect, reinterpret_cast<VALUE>(ptr), &exception);
        if (exception)
        {
            rb_jump_tag(exception);
        }
    }
}

static
void nuodb_connection_mark(void * ptr)
{
    trace("nuodb_connection_mark");

    nuodb_connection_handle * handle = static_cast<nuodb_connection_handle *>(ptr);
    track_ref_count("MARK CONN", handle);

    rb_gc_mark(handle->database);
    rb_gc_mark(handle->username);
    rb_gc_mark(handle->password);
    rb_gc_mark(handle->schema);
}

static
void nuodb_connection_decr_reference_count(nuodb_handle * handle)
{
    trace("nuodb_connection_decr_reference_count");
    decr_reference_count(handle);
}

static
VALUE nuodb_connection_alloc(VALUE klass)
{
    trace("nuodb_connection_alloc");

    nuodb_connection_handle * handle = ALLOC(struct nuodb_connection_handle);
    handle->database = Qnil;
    handle->username = Qnil;
    handle->password = Qnil;
    handle->schema = Qnil;
    handle->timezone = Qnil;

    handle->free_func = RUBY_DATA_FUNC(&nuodb_connection_free);
    handle->atomic = 0;
    handle->parent = Qnil;
    handle->parent_handle = 0;
    handle->pointer = 0;
    incr_reference_count(handle);

    print_address("[ALLOC] connection", handle);

    return Data_Wrap_Struct(klass, nuodb_connection_mark, nuodb_connection_decr_reference_count, handle);
}

static void internal_connection_connect_or_raise(nuodb_connection_handle * handle)
{
    trace("internal_connection_connect_or_raise");

    if (handle->schema != Qnil)
    {
        try
        {
            handle->pointer = Connection::create();
            Properties * props = handle->pointer->allocProperties();
            props->putValue("user", StringValueCStr(handle->username));
            props->putValue("password", StringValueCStr(handle->password));
            props->putValue("schema", StringValueCStr(handle->schema));
            if (!NIL_P(handle->timezone))
            {
                props->putValue("TimeZone", StringValueCStr(handle->timezone));
            }
            handle->pointer->openDatabase(StringValueCStr(handle->database), props);
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(),
                                 "Failed to create database connection (\"%s\", \"%s\", ********, \"%s\"): %s",
                                 StringValueCStr(handle->database),
                                 StringValueCStr(handle->username),
                                 StringValueCStr(handle->schema),
                                 e.getText());
        }
    }
    else
    {
        try
        {
            handle->pointer = Connection::create();
            Properties * props = handle->pointer->allocProperties();
            props->putValue("user", StringValueCStr(handle->username));
            props->putValue("password", StringValueCStr(handle->password));
            if (!NIL_P(handle->timezone))
            {
                props->putValue("TimeZone", StringValueCStr(handle->timezone));
            }
            handle->pointer->openDatabase(StringValueCStr(handle->database), props);
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(),
                                 "Failed to create database connection (\"%s\", \"%s\", ********): %s",
                                 StringValueCStr(handle->database),
                                 StringValueCStr(handle->username),
                                 e.getText());
        }
    }
}

/*
 * call-seq:
 *  commit()
 *
 * Commit the database transaction.
 *
 *      NuoDB::Connection.new (hash) do |connection|
 *          ...
 *          connection.commit
 *      end  #=> automatically disconnected connection
 */
static VALUE nuodb_connection_commit(VALUE self)
{
    trace("nuodb_connection_commit");

    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            handle->pointer->commit();
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to commit transaction: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: connection handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  disconnect()
 *
 * Disconnects the connection.
 */
static VALUE nuodb_connection_disconnect(VALUE self)
{
    trace("nuodb_connection_disconnect");
    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);//;
    track_ref_count("CONN DISCONNECT", handle);
    nuodb_connection_free(handle);
//    if (handle != NULL)
//    {
//        track_ref_count("FREE CONN", handle);
//        if (handle->pointer != NULL)
//        {
//            try
//            {
//                track_ref_count("CLOSE CONN", handle);
//                handle->pointer->close();
//                handle->pointer = NULL;
//            }
//            catch (SQLException & e)
//            {
//                log(DEBUG, "rb_raise");
//                rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close connection: %s", e.getText());
//            }
//        }
//    }
    return Qnil;
//
//
//
//    if (ENABLE_CLOSE_HOOK)
//    {
//        nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
//        if (handle != NULL && handle->pointer != NULL)
//        {
//            nuodb_connection_decr_reference_count(handle);
//        }
//        else
//        {
//            rb_raise(rb_eArgError, "invalid state: connection handle nil");
//        }
//    }
//    return Qnil;
}

/*
 * call-seq:
 *  connection.ping         -> boolean
 *  connection.connected?   -> boolean
 *
 * Returns true if the connection is still alive, otherwise false.
 *
 *  connection.connected?   #=> true
 */
static VALUE nuodb_connection_ping(VALUE self)
{
    trace("nuodb_connection_ping");

    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            handle->pointer->ping();
            return Qtrue;
        }
        catch (SQLException & e)
        {
        }
    }
    return Qfalse;
}

/*
 * call-seq:
 *  prepare(sql) -> PreparedStatement
 *
 * Creates a prepared statement.
 *
 *      NuoDB::Connection.new (hash) do |connection|
 *          connection.prepare 'insert into foo (f1,f2) values (?, ?)' do |statement|
 *              statement.bind_params [...]
 *              statement.execute
 *          end
 *      end  #=> automatically disconnected connection
 */
static VALUE nuodb_connection_prepare(VALUE self, VALUE sql)
{
    trace("nuodb_connection_prepare");

    return nuodb_prepared_statement_initialize(self, sql);
}

/*
 * call-seq:
 *  statement -> Statement
 *
 * Creates a statement.
 *
 * <b>This is a NuoDB-specific extension.</b>
 *
 *      NuoDB::Connection.new (hash) do |connection|
 *          connection.statement do |statement|
 *              statement.execute sql
 *          end
 *      end  #=> automatically disconnected connection
 */
static VALUE nuodb_connection_statement(VALUE self)
{
    trace("nuodb_connection_statement");

    return nuodb_statement_initialize(self);
}

/*
 * call-seq:
 *  rollback()
 *
 * Rollback the database transaction.
 *
 *      NuoDB::Connection.new (hash) do |connection|
 *          ...
 *          connection.rollback
 *      end  #=> automatically disconnected connection
 */
static VALUE nuodb_connection_rollback(VALUE self)
{
    trace("nuodb_connection_rollback");

    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            handle->pointer->rollback();
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to rollback transaction: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: connection handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  autocommit= boolean
 *
 * Sets the connections autocommit state.
 *
 *      NuoDB::Connection.new (hash) do |connection|
 *          connection.autocommit = false
 *          ...
 *          connection.commit
 *      end  #=> automatically disconnected connection
 *
 * <b>This is a NuoDB-specific extension.</b>
 */
static VALUE nuodb_connection_autocommit_set(VALUE self, VALUE value)
{
    trace("nuodb_connection_autocommit_set");

    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        bool auto_commit = !(RB_TYPE_P(value, T_FALSE) || RB_TYPE_P(value, T_NIL));
        try
        {
            handle->pointer->setAutoCommit(auto_commit);
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to set autocommit (%d) for connection: %s", auto_commit, e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: connection handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *  autocommit? -> boolean
 *
 * Gets the connections autocommit state.
 *
 *  unless connection.autocommit? do    #=> false
 *      connection.commit
 *  end
 *
 * <b>This is a NuoDB-specific extension.</b>
 */
static VALUE nuodb_connection_autocommit_get(VALUE self)
{
    trace("nuodb_connection_autocommit_get");

    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
    if (handle != NULL && handle->pointer != NULL)
    {
        try
        {
            bool current = handle->pointer->getAutoCommit();
            return AS_QBOOL(current);
        }
        catch (SQLException & e)
        {
            rb_raise_nuodb_error(e.getSqlcode(), "Failed to determine autocommit state for connection: %s", e.getText());
        }
    }
    else
    {
        rb_raise(rb_eArgError, "invalid state: connection handle nil");
    }
    return Qnil;
}

/*
 * call-seq:
 *
 *  Connection.new(key, value, ...) -> new_connection
 *  Connection.new(key, value, ...) { |connection| block }
 *
 * Creates a new connection using the specified connection parameters. In the
 * first form a new connection is created but the caller is responsible for
 * calling disconnect. In the second form a new connection is created but the
 * caller is not responsible for calling disconnect; the connection is
 * automatically closed after the block exits.
 *
 *      NuoDB::Connection.new (
 *          :database => 'hockey',
 *          :username => 'gretzky',
 *          :password => 'goal!',
 *          :schema   => 'players')     #=> connection
 *      NuoDB::Connection.new (
 *          :database => 'hockey',
 *          :username => 'gretzky',
 *          :password => 'goal!',
 *          :schema   => 'players') { |connection| ... }    #=> automatically disconnected connection
 */
static VALUE nuodb_connection_initialize(VALUE self, VALUE hash)
{
    trace("nuodb_connection_initialize");

    if (TYPE(hash) != T_HASH)
    {
        rb_raise(rb_eTypeError, "wrong argument type %s (Hash expected)", rb_class2name(CLASS_OF(hash)));
    }

    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
    if (NIL_P(handle->database))
    {
        VALUE value = rb_hash_aref(hash, sym_database);
        if (value == Qnil)
        {
            rb_raise(rb_eArgError, "missing database argument for connection: please specify :database => 'your_db_name'");
        }
        if (TYPE(value) != T_STRING)
        {
            rb_raise(rb_eTypeError, "wrong database argument type %s (String expected)", rb_class2name(CLASS_OF(value)));
        }
        handle->database = value;
    }
    if (handle->username == Qnil)
    {
        VALUE value = rb_hash_aref(hash, sym_username);
        if (value == Qnil)
        {
            rb_raise(rb_eArgError, "missing username argument for connection: please specify :username => 'your_db_username'");
        }
        if (TYPE(value) != T_STRING)
        {
            rb_raise(rb_eTypeError, "wrong username argument type %s (String expected)", rb_class2name(CLASS_OF(value)));
        }
        handle->username = value;
    }
    if (handle->password == Qnil)
    {
        VALUE value = rb_hash_aref(hash, sym_password);
        if (value == Qnil)
        {
            rb_raise(rb_eArgError, "missing password argument for connection: please specify :password => 'your_db_password'");
        }
        if (TYPE(value) != T_STRING)
        {
            rb_raise(rb_eTypeError, "wrong password argument type %s (String expected)", rb_class2name(CLASS_OF(value)));
        }
        handle->password = value;
    }
    if (handle->schema == Qnil)
    {
        VALUE value = rb_hash_aref(hash, sym_schema);
        if (TYPE(value) != T_STRING)
        {
            rb_raise(rb_eTypeError, "wrong schema argument type %s (String expected)", rb_class2name(CLASS_OF(value)));
        }
        handle->schema = value;
    }
    if (handle->timezone == Qnil)
    {
        VALUE value = rb_hash_aref(hash, sym_timezone);
        if (value != Qnil)
        {
            if (TYPE(value) != T_STRING)
            {
                rb_raise(rb_eTypeError, "wrong timezone argument type %s (String expected)", rb_class2name(CLASS_OF(value)));
            }
            handle->timezone = value;
        }
    }

    internal_connection_connect_or_raise(handle);

    if (!rb_block_given_p()) {

        trace("nuodb_connection_initialize: no block");

        return self;
    }

    trace("nuodb_connection_initialize: begin block");

    int state = 0;
    VALUE result = rb_protect(rb_yield, self, &state);

    trace("nuodb_connection_initialize: end block");

    // n.b. don't do this as it may introduce crashes of the ruby process !!!
    //nuodb_connection_disconnect(self);

    trace("nuodb_connection_initialize: auto disconnect");

    if (state)
    {
        rb_jump_tag(state);
    }
    else
    {
        return result;
    }

    return Qnil;
}

///*
// * call-seq:
// *  tables(schema = nil) -> Results
// *
// * Retrieves a result set containing the tables associated to the specified schema,
// * or if the specified schema is nil, return a result set of tables for the schema
// * associated with the connection.
// */
//static VALUE nuodb_connection_tables(VALUE self, VALUE schema)
//{
//    trace("nuodb_connection_tables");
//
//    nuodb_connection_handle * handle = cast_handle<nuodb_connection_handle>(self);
//    if (handle != NULL && handle->pointer != NULL)
//    {
//        if (schema == Qnil)
//        {
//            schema = handle->schema;
//        }
////        char const * sql = "SELECT tablename FROM system.tables WHERE schema = ?"
////        try
////        {
////            return nuodb_result_alloc(self, handle->pointer->getGeneratedKeys());
////        }
////        catch (SQLException & e)
////        {
////            rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the tables keys for the schema: %s", e.getText());
////        }
//    }
//    else
//    {
//        rb_raise(rb_eArgError, "invalid state: connection handle nil");
//    }
//    return Qnil;
//}

void nuodb_define_connection_api()
{
    /*
     * Document-class: NuoDB::Connection
     *
     * A Connection object maintains a connection to a specific database.
     * SQL statements are executed and results are returned within the context of a
     * connection.
     */
    nuodb_connection_klass = rb_define_class_under(m_nuodb, "Connection", rb_cObject);

    rb_define_alloc_func(nuodb_connection_klass, nuodb_connection_alloc);
    rb_define_method(nuodb_connection_klass, "initialize", RUBY_METHOD_FUNC(nuodb_connection_initialize), 1);

    sym_username = ID2SYM(rb_intern("username"));
    sym_password = ID2SYM(rb_intern("password"));
    sym_database = ID2SYM(rb_intern("database"));
    sym_schema = ID2SYM(rb_intern("schema"));
    sym_timezone = ID2SYM(rb_intern("timezone"));

    // DBI

    rb_define_method(nuodb_connection_klass, "commit", RUBY_METHOD_FUNC(nuodb_connection_commit), 0);
    //rb_define_method(nuodb_connection_klass, "disconnect", RUBY_METHOD_FUNC(nuodb_connection_disconnect), 0);
    rb_define_method(nuodb_connection_klass, "ping", RUBY_METHOD_FUNC(nuodb_connection_ping), 0);
    rb_define_method(nuodb_connection_klass, "prepare", RUBY_METHOD_FUNC(nuodb_connection_prepare), 1);
    rb_define_method(nuodb_connection_klass, "rollback", RUBY_METHOD_FUNC(nuodb_connection_rollback), 0);
    // todo add .tables, definitely!
    // todo add .columns, or not? If we did: .columns(table_name)
    // todo and tie this into the SchemaCache on the Ruby side.

    // NUODB EXTENSIONS

    //rb_define_method(nuodb_connection_klass, "tables", RUBY_METHOD_FUNC(nuodb_connection_tables), 1);
    rb_define_method(nuodb_connection_klass, "autocommit=", RUBY_METHOD_FUNC(nuodb_connection_autocommit_set), 1);
    rb_define_method(nuodb_connection_klass, "autocommit?", RUBY_METHOD_FUNC(nuodb_connection_autocommit_get), 0);
    rb_define_method(nuodb_connection_klass, "statement", RUBY_METHOD_FUNC(nuodb_connection_statement), 0);
    rb_define_method(nuodb_connection_klass, "connected?", RUBY_METHOD_FUNC(nuodb_connection_ping), 0);
}

//------------------------------------------------------------------------------

/*
 * The NuoDB package provides a Ruby interface to the NuoDB database.
 */
extern "C" void Init_nuodb(void)
{
    /*
     * The NuoDB module contains classes and function definitions enabling use
     * of the NuoDB database.
     */
    m_nuodb = rb_define_module("NuoDB");

    c_nuodb_error = rb_const_get(m_nuodb, rb_intern("DatabaseError"));

    c_error_code_assignment = rb_intern("error_code=");

    nuodb_define_connection_api();

    nuodb_define_statement_api();

    nuodb_define_prepared_statement_api();

    nuodb_define_result_api();
}
