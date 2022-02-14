"""
 Unlike lists lazy iterators do not store their data in memory.
 When working with large datasets or streaming datasets, A generator pattern helps.

"""


def sequence_enumerator(my_data):
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
