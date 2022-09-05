from unittest import TestCase, mock


from lect_02.examples.testing_example.calculate import my_calc


class ExampleTestCase(TestCase):

    def test_5_equal_5(self):
        actual = 5
        expected = 5

        self.assertEqual(expected, actual)

    def test_list(self):
        actual = [1, 2, 3]
        expected = [1, 2, 3]

        self.assertListEqual(expected, actual)

    def test_true(self):
        actual = True

        self.assertTrue(actual)

    def my_test_method(self):
        # doesn't work!
        self.assertEqual(3, 3)


class MyCalcIntegrationTestCase(TestCase):
    """
    Integration test for my_calc function
    """

    def test_when_x_is_5(self):
        x = 5
        actual = my_calc(x)
        expected = 47

        self.assertEqual(expected, actual)


class MyCalcUnitTestCase(TestCase):
    """
    Unit test for my_calc function
    """

    # @mock.patch('lect_02.examples.testing_example.dept_calc.return_42') <-- doesn't work!
    @mock.patch('lect_02.examples.testing_example.calculate.return_42')
    def test_when_x_is_5(self, return_42_mock: mock.MagicMock):
        return_42_mock.return_value = 42

        x = 5
        actual = my_calc(x)
        expected = 47

        self.assertEqual(expected, actual)
