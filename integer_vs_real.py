import numpy as np

# def is_int(val):
#     try:
#         val1 = np.int64(val)
#         if val1 == val:
#             return True
#         else:
#             return False
#     except:
#         return False
#
#
# def is_real(val):
#     if is_int(val):
#         return False
#     try:
#         real_val = float(val)
#         if real_val == np.inf or real_val == -np.inf or real_val == np.nan:
#             return False
#         return True
#     except:
#         return False

def is_int(val):
    # if val is not a string:
    if isinstance(val, int):
        return True
    # if val is a string
    elif isinstance(val, str):
        try:
            int(val)
            return True
        except:
            return False
    else:
        return False

def is_real(val):
    # if val is not a string
    if isinstance(val, float):
        return True
    # if val is a string:
    elif isinstance(val, str):
        if is_int(val):
            return False
        try:
            real_val = float(val)
            if real_val == np.inf or real_val == -np.inf or real_val == np.nan:
                return False
            return True
        except:
            return False
    else:
        return False

# a = 1
# b = '1'
# print(is_int(a))
# print(is_int(b))
# print(is_real(a))
# print(is_real(b))

# a = 1.1
# b = '1.1'
# print(is_int(a))
# print(is_int(b))
# print(is_real(a))
# print(is_real(b))

# a = 1
# b = '1'
# print(isinstance(a, int))
# print(isinstance(b, int))

# a = 1
# b = 1.1
# c = '1'
# d = '1.1'
#
# print(isinstance(a, int))
# print(isinstance(b, int))
# print(isinstance(c, int))
# print(isinstance(d, int))
# print(isinstance(a, float))
# print(isinstance(b, float))
# print(isinstance(c, float))
# print(isinstance(d, float))
# print(isinstance(a, str))
# print(isinstance(b, str))
# print(isinstance(c, str))
# print(isinstance(d, str))