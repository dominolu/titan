"""Small Numba pointer intrinsics used by the callback-only strategy context."""

from numba import types
from numba.core import cgutils
from numba.core.extending import intrinsic


@intrinsic
def address_as_void_pointer(typingctx, src):
    def codegen(context, builder, signature, args):
        return builder.inttoptr(args[0], cgutils.voidptr_t)

    return types.voidptr(src), codegen
