from .. import matrix

def do_test(res):
    sz = res.size()
    s = frozenset([matrix.generate_lists(res.index(i)) for i in range(sz)])
    for i in range(res.size()):
        assert sz == len(s)

def mbs(num, l):
    return matrix.Sum(num*10, [matrix.Base(i + (100*num)) for i in l])

class TestMatrix(object):
    def test_simple(self):
        do_test(mbs(1, range(6)))

    def test_simple2(self):
        do_test(mbs(1, range(5)))

    # The test_product* tests differ by the degree by which dimension
    # sizes share prime factors
    def test_product_simple(self):
        do_test(matrix.Product(1, [mbs(1, range(6)), mbs(2, range(2))]))

    def test_product_3_facets_2_prime_factors(self):
        do_test(matrix.Product(1, [
                    mbs(1, range(6)),
                    mbs(2, range(2)),
                    mbs(3, range(3)),
                    ]))

    def test_product_3_facets_2_prime_factors_one_larger(self):
        do_test(matrix.Product(1, [
                    mbs(1, range(2)),
                    mbs(2, range(5)),
                    mbs(4, range(4)),
                    ]))

    def test_product_4_facets_2_prime_factors(self):
        do_test(matrix.Sum(1, [
                    mbs(1, range(6)),
                    mbs(3, range(3)),
                    mbs(2, range(2)),
                    mbs(4, range(9)),
                    ]))

    def test_product_2_facets_2_prime_factors(self):
        do_test(matrix.Sum(1, [
                    mbs(1, range(2)),
                    mbs(2, range(5)),
                    ]))

    def test_product_with_sum(self):
        do_test(matrix.Sum(
                9,
                [
                    mbs(10, range(6)),
                    matrix.Product(1, [
                            mbs(1, range(2)),
                            mbs(2, range(5)),
                            mbs(4, range(4))]),
                    matrix.Product(8, [
                            mbs(7, range(2)),
                            mbs(6, range(5)),
                            mbs(5, range(4))])
                    ]
                ))
