import locale


def add_comma(num):
    """API to add comma to a number"""
    locale.setlocale(locale.LC_ALL, 'en_IN')
    price = locale.currency(num, grouping=True)
    return price[1:]


def insert_commas(num):
    num_str = str(num)
    result = ""
    for i, digit in enumerate(num_str[::-1]):
        if i > 0:
            if i <= 3 and i % 3 == 0:
                result = "," + result
            if i == 4:
                pass
            elif i > 4 and i % 2 != 0:
                result = "," + result
        result = digit + result
    return result
