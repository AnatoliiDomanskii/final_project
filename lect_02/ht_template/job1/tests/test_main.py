"""
Tests for main.py
# TODO: write tests
"""
from unittest import TestCase, mock

# NB: avoid relative imports when you will write your code
from .. import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()


    @mock.patch('lect_02.ht_template.job1.api.get_sales')
    def test_return_400_date_param_missed_path_param_missed(
            self,
            get_sales_mock: mock.MagicMock
        ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                # no 'date' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_raw_dir_param_missed(self):
        pass

    @mock.patch('lect_02.ht_template.job1.api.get_sales')
    def test_api_get_sales_called(
            self,
            get_sales_mock: mock.MagicMock
    ):
        """
        Test whether api.get_sales is called with proper params
        """
        fake_date = '1970-01-01'
        self.client.post(
            '/',
            json={
                'date': fake_date,
                'target_dir': '/foo/bar/',
            },
        )

        get_sales_mock.assert_called_with(date=fake_date)

    @mock.patch('lect_02.ht_template.job1.api.get_sales')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock
    ):
        pass
