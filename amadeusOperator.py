from amadeus import Client, ResponseError
import pandas as pd
from datetime import datetime, timedelta


class AmadeusOperator:
    """
    A class to interact with the Amadeus API for searching and retrieving flight offers and other travel-related data.
    """
    def __init__(self, amadeus_api_key, amadeus_api_secret):
        """
        Initializes the AmadeusOperator with the provided API key and secret.

        Parameters:
        amadeus_api_key (str): Your Amadeus API key for authentication.
        amadeus_api_secret (str): Your Amadeus API secret for authentication.
        """
        self.amadeus = Client(
            client_id=amadeus_api_key,
            client_secret=amadeus_api_secret
        )
        return

    def get_flight_offers(self, origin, destination, departure_date, filepath, adults=1):
        """
        Retrieves the cheapest flight offers for a given journey.

        Parameters:
        origin (str): The IATA code for the origin city/airport.
        destination (str): The IATA code for the destination city/airport.
        departure_date (str): The departure date in the format 'YYYY-MM-DD'.
        adults (int, optional): The number of adult passengers. Default is 1.

        Returns:
        list: A list of flight offers if available, otherwise an empty list.
        """
        # Check if the departure date is before today's date
        departure_date_obj = datetime.strptime(departure_date, '%Y-%m-%d').date()
        today_date = datetime.today().date()
        if departure_date_obj < today_date:
            print(f"Error: Departure date {departure_date} is in the past.")
            return None

        try:
            response = self.amadeus.shopping.flight_offers_search.get(
                originLocationCode=origin,
                destinationLocationCode=destination,
                departureDate=departure_date,
                adults=adults
            )

            # Process the response and extract the flight offers
            offer_lst = response.result['data']

            # Processing
            df = pd.DataFrame()
            df = self._add_offers_to_df(df, offer_lst, max_price=400)
            df.to_csv(filepath, mode='a', header=False, index=False)
            return offer_lst

        except ResponseError as e:
            print(f"An error occurred: {e}\nDeparture: {origin}. Destination: {destination}")
            print(e.code)
            return None

    def get_flights_within_dates(self, origin, destination, start_date, end_date, filepath, adults=1):
        """
        Retrieves flight offers over a range of dates. The results are saved in the provided filepath.

        Parameters:
        origin (str): The IATA code for the origin city/airport.
        destination (str): The IATA code for the destination city/airport.
        start_date (datetime.date): The starting date of the date range.
        end_date (datetime.date): The ending date of the date range.
        filepath (str): The path to the file where the flight offers will be stored or processed.
        adults (int, optional): The number of adult passengers. Default is 1.
        """
        current_date = start_date
        while current_date <= end_date:
            current_date_formatted = current_date.strftime('%Y-%m-%d')
            self.get_flight_offers(origin, destination, departure_date=current_date_formatted, filepath=filepath, adults=adults)
            current_date += timedelta(days=1)
        return

    def get_flights_across_destinations(self, origin, destination_lst, start_date, end_date, filepath, adults=1):
        """
        Retrieves flight offers over a range of destinations. The results are saved in the provided filepath.

        Parameters:
        origin (str): The IATA code for the origin city/airport.
        destination_lst (list): The list of IATA codes for the destination city/airport.
        start_date (datetime.date): The starting date of the date range.
        end_date (datetime.date): The ending date of the date range.
        filepath (str): The path to the file where the flight offers will be stored or processed.
        adults (int, optional): The number of adult passengers. Default is 1.
        """
        for destination in destination_lst:
            self.get_flights_within_dates(origin, destination, start_date, end_date, filepath, adults=adults)
            print(f"{origin} - {destination} is retrieved.")
        return

    def get_flights_across_origins(self, origin_lst, destination, start_date, end_date, filepath, adults=1):
        """
        Retrieves flight offers over a range of origins. The results are saved in the provided filepath.

        Parameters:
        origin_lst (list): The list if IATA codes for the origin city/airport.
        destination (str): The IATA code for the destination city/airport.
        start_date (datetime.date): The starting date of the date range.
        end_date (datetime.date): The ending date of the date range.
        filepath (str): The path to the file where the flight offers will be stored or processed.
        adults (int, optional): The number of adult passengers. Default is 1.
        """
        for origin in origin_lst:
            self.get_flights_within_dates(origin, destination, start_date, end_date, filepath, adults=adults)
            print(f"{origin} - {destination} is retrieved.")
        return

    @staticmethod
    def _add_offers_to_df(df, offer_lst, max_price):
        """
        Processes a list of flight offers and adds relevant data to a DataFrame.

        Parameters:
        df (pandas.DataFrame): The DataFrame to which flight offers will be added.
        offer_lst (list): A list of flight offers returned by the Amadeus API.
        max_price (float): The maximum price threshold for filtering flight offers.

        Returns:
        pandas.DataFrame: The updated DataFrame containing the filtered flight offers.
        """
        for offer in offer_lst:
            itinerary = offer['itineraries'][0]
            total_price = offer['price']['total'] # Price
            try:
                total_price = float(total_price)
            except ValueError:
                continue
            if total_price > max_price:
                continue
            currency = offer['price']['currency'] # Currency
            total_duration = itinerary['duration'] # Total duration

            segments = itinerary['segments']  # Segments
            num_of_stops = len(segments) - 1 # Number of stops
            if num_of_stops > 1:
                continue

            date_depart = segments[0]['departure']['at']
            iata0_depart = segments[0]['departure']['iataCode'] # Departure city
            iata0_arrive = segments[0]['arrival']['iataCode']
            segment0_duration = segments[0]['duration']

            offer_to_add = {
                'Currency': currency,
                'Price': total_price,
                'Date': date_depart,
                'Stops': num_of_stops,
                'IATA_Origin': iata0_depart,
                'IATA_Destination': iata0_arrive,
                'Duration': segment0_duration,
                'IATA1_Origin': '',
                'IATA1_Destination': '',
                'Duration1': '',
                'Total_Duration': total_duration,
            }

            if num_of_stops > 0:
                iata1_depart = segments[1]['departure']['iataCode']  # Departure city
                iata1_arrive = segments[1]['arrival']['iataCode']
                segment1_duration = segments[1]['duration']

                offer_to_add['IATA1_Origin'] = iata1_depart
                offer_to_add['IATA1_Destination'] = iata1_arrive
                offer_to_add['Duration1'] = segment1_duration

            df = pd.concat([df, pd.DataFrame([offer_to_add])], ignore_index=True)
        return df