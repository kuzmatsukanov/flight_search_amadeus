import os
import json
import pandas as pd
import numpy as np

class FlightOfferProcessor:
    """
    A class to process flight offers and store the data in a DataFrame.
    """
    def __init__(self, filepath):
        """
        Initializes the FlightOfferProcessor with a file path and reads the initial data into a DataFrame.

        Parameters:
        filepath (str): The path to the file where flight data is stored or will be stored.
        """
        self.filepath = filepath
        self.df = self.read_file()
        pass

    def read_file(self):
        """
        Reads a CSV file from the specified filepath and loads its contents into a DataFrame.

        Returns:
        pandas.DataFrame or None: The DataFrame containing the data from the CSV file if successful.
        If the file is not found, returns None and prints an error message.
        """
        try:
            df = pd.read_csv(self.filepath)
            return df
        except FileNotFoundError:
            print(f"Error: The file '{self.filepath}' does not exist.")
            return None

    def process(self):
        """
        Processes the DataFrame by applying a series of transformations and updates.

        This method performs several steps on the DataFrame `df`:
        1. Adds headers to the DataFrame.
        2. Maps IATA codes to city names.
        3. Processes and formats duration information.
        4. Formats dates.
        5. Calculates stop durations.
        6. Converts prices from EUR to ILS.
        7. Rounds prices to a standard format.
        8. Reorders the columns in the DataFrame.

        Returns:
        None
        """
        if self.df is None:
            print("Warning: No DataFrame available. Processing cannot proceed.")
            return
        self.add_header()
        self.iata2city()
        self.process_duration()
        self.format_date()
        self.get_stop_duration()
        self.eur2ils()
        self.round_price()
        self.reorder_columns()
        pass

    def add_header(self):
        """
        Adds header to the DataFrame
        """
        self.df.columns = ['Currency', 'Price', 'Date', 'Stops', 'IATA_Origin', 'IATA_Destination',
                      'Duration', 'IATA1_Origin', 'IATA1_Destination', 'Duration1',
                      'Total_Duration']

    def iata2city(self):
        """
        Maps IATA code to the correspondent city name
        """
        iata_city = self._get_iata_city_code()
        self.df['City_Origin'] = self.df['IATA_Origin'].map(iata_city)
        self.df['City_Destination'] = self.df['IATA_Destination'].map(iata_city)
        self.df['City1_Origin'] = self.df['IATA1_Origin'].map(iata_city)
        self.df['City1_Destination'] = self.df['IATA1_Destination'].map(iata_city)
        pass

    def process_duration(self):
        """
        Converts the Duration format into 'HH:MM'
        """
        self.df['Duration'] = self._format_duration(self.df['Duration'])
        self.df['Duration1'] = self._format_duration(self.df['Duration1'])
        self.df['Total_Duration'] = self._format_duration(self.df['Total_Duration'])
        pass

    def get_stop_duration(self):
        """
        Calculates the stop's duration
        """
        self.df['Stop_Duration'] =\
            (pd.to_timedelta(self.df['Total_Duration'] + ':00') -
             pd.to_timedelta(self.df['Duration'] + ':00') -
             pd.to_timedelta(self.df['Duration1'] + ':00'))
        self.df['Stop_Duration'] = self._format_duration(self.df['Stop_Duration'])
        pass

    @staticmethod
    def _format_duration(duration_series):
        """
        Converts pd.Series of Duration into 'HH:MM'

        Parameters:
        duration_series (pandas.Series): A series of duration strings in ISO 8601 format (e.g., 'PT9H45M').

        Returns:
        pandas.Series: A series of formatted duration strings in 'HH:MM' format.
        """
        timedeltas = pd.to_timedelta(duration_series)
        formatted_series = timedeltas.apply(
            lambda x: f'{x.components.days * 24 + x.components.hours:02}:{x.components.minutes:02}'
            if pd.notnull(x) else np.nan
        )
        return formatted_series

    @staticmethod
    def _get_iata_city_code(jsonfilepath='iata_codes.json'):
        """
        Load iata-city json

        Parameters:
        jsonfilepath (str): path of json file
        """
        with open(jsonfilepath) as file:
            airport_city = json.load(file)
        return airport_city

    def format_date(self):
        """
        Format the date into 'YY-mm-dd HH-MM' format
        """
        self.df['Date'] = pd.to_datetime(self.df['Date']).dt.strftime('%Y-%m-%d %H:%M')
        pass

    def round_price(self):
        """
        Round the prices
        """
        self.df['Price'] = self.df['Price'].round()
        pass

    def eur2ils(self):
        """
        Convert EUR to ILS
        """
        if not self.df['Currency'].eq('EUR').all():
            print("Not all prices are in EUR!!!")
        else:
            exchange_rate = 4.14
            self.df['Price'] = self.df['Price'] * exchange_rate
            self.df['Currency'] = 'ILS'

    def reorder_columns(self):
        """
        Reorder the columns in the dataframe
        """
        new_order = ['Currency', 'Price', 'Date', 'Stops', 'Total_Duration', 'Stop_Duration',
                     'City_Origin','IATA_Origin', 'City_Destination', 'IATA_Destination', 'Duration',
                     'IATA1_Destination', 'City1_Destination', 'Duration1']
        self.df = self.df[new_order]
        pass

    def save_csv(self, filepath):
        """
        Saves the DataFrame into the file of the specified path

        Params:
        filepath (str): file path
        """
        if os.path.exists(filepath):
            print(f"Warning: The file '{filepath}' already exists. The file is not saved.")
            return
        self.df.to_csv(filepath, index=False)
        pass
