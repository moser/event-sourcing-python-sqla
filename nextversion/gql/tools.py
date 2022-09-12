import asyncio
from functools import wraps, partial

import strawberry


def force_run_in_executor(func):
    """
    A decorator that converts a synchronous function into an asynchronous one
    by wrapping it in a function that will run it in the default executor of
    the current event loop.
    """
    # source https://stackoverflow.com/questions/43241221/how-can-i-wrap-a-synchronous-function-in-an-async-coroutine
    import inspect

    assert not inspect.iscoroutinefunction(func)

    @wraps(func)
    async def run(*args, **kwargs):
        loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        res = await loop.run_in_executor(None, pfunc)
        return res

    return run


def check_schema(schema: strawberry.Schema, strict: bool = False):
    """
    Finds sync resolvers on a strawberry schema.

    If `strict=True`, raises an error.
    """
    import inspect

    violations = []
    types = [schema._schema.query_type]
    if schema._schema.mutation_type:
        types.append(schema._schema.mutation_type)
    for type_ in types:
        for fname, field in type_.fields.items():
            if not inspect.iscoroutinefunction(field.resolve):
                print(f"Found sync resolver on schema: {type_}.{fname}")
                violations.append((type_, fname))
    if strict and violations:
        raise RuntimeError("Found sync resolvers on schema", violations)
