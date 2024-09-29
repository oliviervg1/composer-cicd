import unittest

from unittest.mock import patch, call, MagicMock
from datetime import datetime, timedelta

#Import the task functions from your DAG file
from london_weather_to_bigquery import fetch_time, fetch_weather, insert_weather_to_bigquery


class TestLondonWeatherDAG(unittest.TestCase):

    @patch('requests.get')
    def test_fetch_time(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'utc_datetime': '2024-10-27T10:00:00+00:00',
            'day_of_week': 'Sunday',
            'week_number': 43,
            'day_of_year': 301
        }
        mock_get.return_value = mock_response

        response = fetch_time.function()

        self.assertEqual(response['utc_datetime'], '2024-10-27T10:00:00+00:00')
        self.assertEqual(response['day_of_week'], 'Sunday')
        self.assertEqual(response['week_number'], 43)
        self.assertEqual(response['day_of_year'], 301)

    @patch('openmeteo_requests.Client')
    def test_fetch_weather(self, mock_client):
        mock_response = MagicMock()
        mock_response.Current.return_value.Variables.side_effect = lambda i: MagicMock(Value=lambda: i+10)
        mock_client.return_value.weather_api.return_value = [mock_response]

        mock_response = fetch_weather.function()

        self.assertEqual(mock_response['temperature_2m'], 10)
        self.assertEqual(mock_response['apparent_temperature'], 11)
        self.assertEqual(mock_response['is_day'], 12)
        self.assertEqual(mock_response['precipitation'], 13)
        self.assertEqual(mock_response['wind_speed_10m'], 14)
        self.assertEqual(mock_response['wind_direction_10m'], 15)

    @patch.dict('os.environ', {'AIRFLOW_VAR_GCP_PROJECT_ID': 'project123', 'AIRFLOW_VAR_BIGQUERY_DESTINATION_TABLE': 'project.dataset.table'})
    @patch('bigframes.pandas.concat')
    @patch('bigframes.pandas.DataFrame')
    def test_insert_weather_to_bigquery(self, mock_concat, mock_df):
        time_mock = {'utc_datetime': '2024-10-27T10:00:00+00:00', 'day_of_week': 'Sunday', 'week_number': 43, 'day_of_year': 301}
        weather_mock = {'lat_long': '51.5085, -0.1257', 'temperature_2m': 15, 'apparent_temperature': 17, 'is_day': 1, 'precipitation': 0, 'wind_speed_10m': 10, 'wind_direction_10m': 270}

        insert_weather_to_bigquery.function(time_mock, weather_mock)

        mock_concat.assert_has_calls([
            call([time_mock]),
            call([weather_mock]),
            call().__getitem__(['utc_datetime', 'day_of_week', 'week_number', 'day_of_year'])
        ])
        
        mock_df.return_value.to_gbq.assert_called_once_with(destination_table='project.dataset.table', if_exists='append')


if __name__ == '__main__':
    unittest.main()
