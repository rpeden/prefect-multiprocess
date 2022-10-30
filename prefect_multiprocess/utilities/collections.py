# async version of prefect.utilities.collections.visit_collection
from collections.abc import Iterator as IteratorABC
from dataclasses import fields, is_dataclass
from typing import Any, Callable, OrderedDict, cast
from unittest.mock import Mock

import pydantic


async def visit_collection_async(
    expr,
    visit_fn: Callable[[Any], Any],
    return_data: bool = False,
    max_depth: int = -1,
):
    """
    This function visits every element of an arbitrary Python collection. If an element
    is a Python collection, it will be visited recursively. If an element is not a
    collection, `visit_fn` will be called with the element. The return value of
    `visit_fn` can be used to alter the element if `return_data` is set.

    Note that when using `return_data` a copy of each collection is created to avoid
    mutating the original object. This may have significant performance penalities and
    should only be used if you intend to transform the collection.

    Supported types:
    - List
    - Tuple
    - Set
    - Dict (note: keys are also visited recursively)
    - Dataclass
    - Pydantic model
    - Prefect annotations

    Args:
        expr (Any): a Python object or expression
        visit_fn (Callable[[Any], Awaitable[Any]]): an async function that
            will be applied to every non-collection element of expr.
        return_data (bool): if `True`, a copy of `expr` containing data modified
            by `visit_fn` will be returned. This is slower than `return_data=False`
            (the default).
        max_depth: Controls the depth of recursive visitation. If set to zero, no
            recursion will occur. If set to a positive integer N, visitation will only
            descend to N layers deep. If set to any negative integer, no limit will be
            enforced and recursion will continue until terminal items are reached. By
            default, recursion is unlimited.
    """

    async def visit_nested(expr):
        # Utility for a recursive call, preserving options and updating the depth.
        return await visit_collection_async(
            expr,
            visit_fn=visit_fn,
            return_data=return_data,
            max_depth=max_depth - 1,
        )

    # Visit every expression
    result = await visit_fn(expr)
    if return_data:
        # Only mutate the expression while returning data, otherwise it could be null
        expr = result

    # Then, visit every child of the expression recursively

    # If we have reached the maximum depth, do not perform any recursion
    if max_depth == 0:
        return result if return_data else None

    # Get the expression type; treat iterators like lists
    typ = list if isinstance(expr, IteratorABC) else type(expr)
    typ = cast(type, typ)  # mypy treats this as 'object' otherwise and complains

    # Then visit every item in the expression if it is a collection
    if isinstance(expr, Mock):
        # Do not attempt to recurse into mock objects
        result = expr

    elif typ in (list, tuple, set):
        items = [await visit_nested(o) for o in expr]
        result = typ(items) if return_data else None

    elif typ in (dict, OrderedDict):
        assert isinstance(expr, (dict, OrderedDict))  # typecheck assertion
        items = [
            (await visit_nested(k), await visit_nested(v)) for k, v in expr.items()
        ]
        result = typ(items) if return_data else None

    elif is_dataclass(expr) and not isinstance(expr, type):
        values = [await visit_nested(getattr(expr, f.name)) for f in fields(expr)]
        items = {field.name: value for field, value in zip(fields(expr), values)}
        result = typ(**items) if return_data else None

    elif isinstance(expr, pydantic.BaseModel):
        # NOTE: This implementation *does not* traverse private attributes
        # Pydantic does not expose extras in `__fields__` so we use `__fields_set__`
        # as well to get all of the relevant attributes
        model_fields = expr.__fields_set__.union(expr.__fields__)
        items = [await visit_nested(getattr(expr, key)) for key in model_fields]

        if return_data:
            # Collect fields with aliases so reconstruction can use the correct field
            # name
            aliases = {
                key: value.alias
                for key, value in expr.__fields__.items()
                if value.has_alias
            }

            model_instance = typ(
                **{
                    aliases.get(key) or key: value
                    for key, value in zip(model_fields, items)
                }
            )

            # Private attributes are not included in `__fields_set__` but we do not want
            # to drop them from the model so we restore them after constructing a new
            # model
            for attr in expr.__private_attributes__:
                # Use `object.__setattr__` to avoid errors on immutable models
                object.__setattr__(model_instance, attr, getattr(expr, attr))

            result = model_instance
        else:
            result = None

    else:
        result = result if return_data else None

    return result
