def get_last_or_null(lst):
    if len(lst) == 0:
        return None
    return lst[len(lst) - 1]