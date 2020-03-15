# -*- coding: utf8 -*-

import random

# https://habr.com/ru/post/346930/
# https://rcoh.me/posts/linear-time-median-finding/

def nlogn_median(l):
    ll = len(l)
    half = ll // 2
    l.sort()
    if not ll % 2:
        return (l[half - 1] + l[half]) * 0.5
    return l[half]

def quickselect(l, k, pivot_fn):
    """
    Выбираем k-тый элемент в списке l (с нулевой базой)
    :param l: список числовых данных
    :param k: индекс
    :param pivot_fn: функция выбора pivot, по умолчанию выбирает случайно
    :return: k-тый элемент l
    """
    if len(l) == 1:
        assert k == 0
        return l[0]

    pivot = pivot_fn(l)

    lows = [el for el in l if el < pivot]
    highs = [el for el in l if el > pivot]
    pivots = [el for el in l if el == pivot]

    llows = len(lows)
    lpivo = len(pivots)

    if k < llows:
        return quickselect(lows, k, pivot_fn)
    elif k < llows + lpivo:
        # Нам повезло и мы угадали медиану
        return pivots[0]
    else:
        return quickselect(highs, k - llows - lpivo, pivot_fn)

def pick_pivot(l):
    """
    Выбираем хорошй pivot в списке чисел l
    Этот алгоритм выполняется за время O(n).
    """
    assert len(l) > 0

    # Если элементов < 5, просто возвращаем медиану
    if len(l) < 5:
        # В этом случае мы возвращаемся к первой написанной нами функции медианы. 
        # Поскольку мы выполняем её только для списка из пяти или менее элементов, она не 
        # зависит от длины входных данных и может считаться постоянным
        # временем.
        return nlogn_median(l)

    # Сначала разделим l на группы по 5 элементов. O(n)
    chunks = chunked(l, 5)

    # Для простоты мы можем отбросить все группы, которые не являются полными. O(n)
    full_chunks = [chunk for chunk in chunks if len(chunk) == 5]


    # Затем мы сортируем каждый фрагмент. Каждая группа имеет фиксированную длину, поэтому каждая сортировка
    # занимает постоянное время. Поскольку у нас есть n/5 фрагментов, эта операция 
    # тоже O(n)
    sorted_groups = [sorted(chunk) for chunk in full_chunks]

    # Медиана каждого фрагмента имеет индекс 2
    medians = [chunk[2] for chunk in sorted_groups]

    # Возможно, я немного повторюсь, но я собираюсь доказать, что нахождение
    # медианы списка можно произвести за доказуемое O(n).
    # Мы находим медиану списка длиной n/5, поэтому эта операция также O(n)
    # Мы передаём нашу текущую функцию pick_pivot в качестве создателя pivot алгоритму
    # quickselect. O(n)
    median_of_medians = quickselect_median(medians, pick_pivot)
    return median_of_medians

# pivot_fn may be random.choice
def quickselect_median(l, pivot_fn=pick_pivot):
    ll = len(l)
    half = ll // 2
    if ll % 2 == 1:
        return quickselect(l, half, pivot_fn)
    else:
        return 0.5 * (quickselect(l, half - 1, pivot_fn) +
                      quickselect(l, half, pivot_fn))

def chunked(l, chunk_size):
    """Разделяем список `l` на фрагменты размером `chunk_size`."""
    return [l[i:i + chunk_size] for i in range(0, len(l), chunk_size)]

# vim: sw=2 sw=2 et
