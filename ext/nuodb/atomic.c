#include "atomic.h"

#if !defined(_WIN32) && !defined(HAVE_GCC_ATOMIC_BUILTINS)
rb_atomic_t
ruby_atomic_exchange(rb_atomic_t *ptr, rb_atomic_t val)
{
    rb_atomic_t old = *ptr;
    *ptr = val;
    return old;
}
#endif
