import os
from dotenv import load_dotenv
load_dotenv()

from datetime import date
from amadeusOperator import AmadeusOperator
from flightOfferProcessor import FlightOfferProcessor

# Input data
destination = 'TLV'
origin_lst = list(FlightOfferProcessor._get_iata_city_code('iata_codes_destinations.json').keys())[1:]
start_date = date(2025, 1, 3)
end_date = date(2025, 1, 4)

# Retrieve the flight offers
FILEPATH = "raw_offers_back.csv"
amd = AmadeusOperator(os.getenv('AMADEUS_API_KEY'), os.getenv('AMADEUS_API_SECRET'))
amd.get_flights_across_origins(origin_lst, destination, start_date, end_date, FILEPATH, adults=1)

# Process the retrieved flight offers
fp = FlightOfferProcessor('raw_offers_back.csv')
fp.process()
fp.save_csv('offers_back.csv')
