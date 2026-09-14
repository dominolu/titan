"""Small Numba pointer intrinsics used by the callback-only strategy context."""

from numba import types
from numba.core import cgutils
from numba.core.extending import intrinsic
from llvmlite import ir


@intrinsic
def address_as_void_pointer(typingctx, src):
    def codegen(context, builder, signature, args):
        return builder.inttoptr(args[0], cgutils.voidptr_t)

    return types.voidptr(src), codegen


@intrinsic
def call_execution_host(typingctx, function_address, context_address, account_no,
                        request_address, task_id_address):
    """Call a Rust execution host function whose address is supplied by the ABI context."""
    signature = types.int32(
        function_address,
        context_address,
        account_no,
        request_address,
        task_id_address,
    )

    def codegen(context, builder, _signature, args):
        i32 = context.get_value_type(types.int32)
        i64 = context.get_value_type(types.uint64)
        function_type = ir.FunctionType(
            i32,
            [cgutils.voidptr_t, i32, cgutils.voidptr_t, i64.as_pointer()],
        )
        function = builder.inttoptr(args[0], function_type.as_pointer())
        host_context = builder.inttoptr(args[1], cgutils.voidptr_t)
        request = builder.inttoptr(args[3], cgutils.voidptr_t)
        task_id = builder.inttoptr(args[4], i64.as_pointer())
        account = args[2] if args[2].type == i32 else builder.trunc(args[2], i32)
        return builder.call(function, [host_context, account, request, task_id])

    return signature, codegen
