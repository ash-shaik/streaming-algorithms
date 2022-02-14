"""
 Unlike lists lazy iterators do not store their data in memory.
 When working with large datasets or streaming datasets, A generator pattern helps.

"""


def sequence_enumerator(my_data):
    """
    The yield statement turns a functions into a generator.
    A generator is a function which returns a generator object.
    As soon as "next" is called again on the generator object,
    the generator function will resume execution right after
    the yield statement in the code, where the last call is made.
    The execution will continue in the state in which the generator was left after the last yield
    :param my_data:
    :return:
    """
    index = 0
    while True:
        yield index, my_data[index]
        index += 1


if __name__ == '__main__':
    str_data = "azxlmn"
    s_iter = sequence_enumerator(str_data)

    for x in range(0, len(str_data)):
        index_, letter = next(s_iter)
        print(f'{index_}: {letter}')
