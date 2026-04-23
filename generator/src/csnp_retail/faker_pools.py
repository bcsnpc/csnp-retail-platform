"""Static curated data pools used across all entity generators.

Geography records: ~350 postal entries across US / CA / UK / MX.
Format per row:
  (postal_code, city, metro, state_province, state_code,
   country, country_code, currency_code, latitude, longitude,
   timezone, tax_rate)
"""

from __future__ import annotations

# ── Geography pool ────────────────────────────────────────────────────────────
# tax_rate = combined state+local sales tax (US); 0.0 for CA/UK/MX (VAT/GST
# handled separately or excluded from this field for simplicity).

GEO_RECORDS: list[tuple] = [
    # ── United States ─────────────────────────────────────────────────────────
    # California — Los Angeles Metro
    ("90001", "Los Angeles", "Los Angeles", "California", "CA", "United States", "US", "USD", 33.9731, -118.2479, "America/Los_Angeles", 0.1025),
    ("90036", "Los Angeles", "Los Angeles", "California", "CA", "United States", "US", "USD", 34.0703, -118.3434, "America/Los_Angeles", 0.1025),
    ("90210", "Beverly Hills", "Los Angeles", "California", "CA", "United States", "US", "USD", 34.0901, -118.4065, "America/Los_Angeles", 0.1025),
    ("90266", "Manhattan Beach", "Los Angeles", "California", "CA", "United States", "US", "USD", 33.8847, -118.4109, "America/Los_Angeles", 0.1025),
    ("91101", "Pasadena", "Los Angeles", "California", "CA", "United States", "US", "USD", 34.1478, -118.1445, "America/Los_Angeles", 0.1025),
    ("90401", "Santa Monica", "Los Angeles", "California", "CA", "United States", "US", "USD", 34.0195, -118.4912, "America/Los_Angeles", 0.1025),
    ("91364", "Woodland Hills", "Los Angeles", "California", "CA", "United States", "US", "USD", 34.1686, -118.6009, "America/Los_Angeles", 0.1025),
    # California — San Francisco Bay Area
    ("94102", "San Francisco", "San Francisco Bay Area", "California", "CA", "United States", "US", "USD", 37.7790, -122.4177, "America/Los_Angeles", 0.0863),
    ("94107", "San Francisco", "San Francisco Bay Area", "California", "CA", "United States", "US", "USD", 37.7748, -122.3948, "America/Los_Angeles", 0.0863),
    ("94117", "San Francisco", "San Francisco Bay Area", "California", "CA", "United States", "US", "USD", 37.7698, -122.4420, "America/Los_Angeles", 0.0863),
    ("94301", "Palo Alto", "San Francisco Bay Area", "California", "CA", "United States", "US", "USD", 37.4419, -122.1430, "America/Los_Angeles", 0.0875),
    ("94601", "Oakland", "San Francisco Bay Area", "California", "CA", "United States", "US", "USD", 37.7745, -122.2154, "America/Los_Angeles", 0.1025),
    ("94114", "San Francisco", "San Francisco Bay Area", "California", "CA", "United States", "US", "USD", 37.7609, -122.4350, "America/Los_Angeles", 0.0863),
    # California — San Diego
    ("92101", "San Diego", "San Diego", "California", "CA", "United States", "US", "USD", 32.7237, -117.1504, "America/Los_Angeles", 0.0775),
    ("92103", "San Diego", "San Diego", "California", "CA", "United States", "US", "USD", 32.7484, -117.1617, "America/Los_Angeles", 0.0775),
    ("92130", "San Diego", "San Diego", "California", "CA", "United States", "US", "USD", 32.9595, -117.2071, "America/Los_Angeles", 0.0775),
    # California — Sacramento / Other
    ("95814", "Sacramento", "Sacramento", "California", "CA", "United States", "US", "USD", 38.5816, -121.4944, "America/Los_Angeles", 0.0875),
    ("93101", "Santa Barbara", "Santa Barbara", "California", "CA", "United States", "US", "USD", 34.4208, -119.6982, "America/Los_Angeles", 0.0800),
    # Texas — Houston
    ("77001", "Houston", "Houston", "Texas", "TX", "United States", "US", "USD", 29.7604, -95.3698, "America/Chicago", 0.0825),
    ("77019", "Houston", "Houston", "Texas", "TX", "United States", "US", "USD", 29.7565, -95.4085, "America/Chicago", 0.0825),
    ("77098", "Houston", "Houston", "Texas", "TX", "United States", "US", "USD", 29.7371, -95.4139, "America/Chicago", 0.0825),
    ("77494", "Katy", "Houston", "Texas", "TX", "United States", "US", "USD", 29.7858, -95.8245, "America/Chicago", 0.0825),
    # Texas — Dallas / Fort Worth
    ("75201", "Dallas", "Dallas–Fort Worth", "Texas", "TX", "United States", "US", "USD", 32.7767, -96.7970, "America/Chicago", 0.0825),
    ("75205", "Dallas", "Dallas–Fort Worth", "Texas", "TX", "United States", "US", "USD", 32.8355, -96.7864, "America/Chicago", 0.0825),
    ("76011", "Arlington", "Dallas–Fort Worth", "Texas", "TX", "United States", "US", "USD", 32.7357, -97.1081, "America/Chicago", 0.0825),
    ("76102", "Fort Worth", "Dallas–Fort Worth", "Texas", "TX", "United States", "US", "USD", 32.7555, -97.3308, "America/Chicago", 0.0825),
    # Texas — Austin / San Antonio
    ("78701", "Austin", "Austin", "Texas", "TX", "United States", "US", "USD", 30.2672, -97.7431, "America/Chicago", 0.0825),
    ("78731", "Austin", "Austin", "Texas", "TX", "United States", "US", "USD", 30.3601, -97.7606, "America/Chicago", 0.0825),
    ("78201", "San Antonio", "San Antonio", "Texas", "TX", "United States", "US", "USD", 29.4241, -98.4936, "America/Chicago", 0.0825),
    ("78209", "San Antonio", "San Antonio", "Texas", "TX", "United States", "US", "USD", 29.4752, -98.4585, "America/Chicago", 0.0825),
    # Florida — Miami
    ("33101", "Miami", "Miami", "Florida", "FL", "United States", "US", "USD", 25.7617, -80.1918, "America/New_York", 0.0700),
    ("33131", "Miami", "Miami", "Florida", "FL", "United States", "US", "USD", 25.7617, -80.1918, "America/New_York", 0.0700),
    ("33139", "Miami Beach", "Miami", "Florida", "FL", "United States", "US", "USD", 25.7907, -80.1300, "America/New_York", 0.0700),
    ("33301", "Fort Lauderdale", "Miami", "Florida", "FL", "United States", "US", "USD", 26.1224, -80.1373, "America/New_York", 0.0700),
    # Florida — Orlando / Tampa
    ("32801", "Orlando", "Orlando", "Florida", "FL", "United States", "US", "USD", 28.5383, -81.3792, "America/New_York", 0.0650),
    ("33602", "Tampa", "Tampa", "Florida", "FL", "United States", "US", "USD", 27.9506, -82.4572, "America/New_York", 0.0750),
    ("33629", "Tampa", "Tampa", "Florida", "FL", "United States", "US", "USD", 27.9217, -82.5073, "America/New_York", 0.0750),
    # New York — NYC Metro
    ("10001", "New York", "New York City", "New York", "NY", "United States", "US", "USD", 40.7484, -74.0008, "America/New_York", 0.0888),
    ("10013", "New York", "New York City", "New York", "NY", "United States", "US", "USD", 40.7195, -74.0039, "America/New_York", 0.0888),
    ("10019", "New York", "New York City", "New York", "NY", "United States", "US", "USD", 40.7659, -73.9838, "America/New_York", 0.0888),
    ("10036", "New York", "New York City", "New York", "NY", "United States", "US", "USD", 40.7575, -73.9921, "America/New_York", 0.0888),
    ("10128", "New York", "New York City", "New York", "NY", "United States", "US", "USD", 40.7792, -73.9490, "America/New_York", 0.0888),
    ("11201", "Brooklyn", "New York City", "New York", "NY", "United States", "US", "USD", 40.6928, -73.9903, "America/New_York", 0.0888),
    ("10301", "Staten Island", "New York City", "New York", "NY", "United States", "US", "USD", 40.6501, -74.0940, "America/New_York", 0.0888),
    # New York — Upstate
    ("14201", "Buffalo", "Buffalo", "New York", "NY", "United States", "US", "USD", 42.8864, -78.8784, "America/New_York", 0.0800),
    # Illinois — Chicago
    ("60601", "Chicago", "Chicago", "Illinois", "IL", "United States", "US", "USD", 41.8858, -87.6181, "America/Chicago", 0.1075),
    ("60611", "Chicago", "Chicago", "Illinois", "IL", "United States", "US", "USD", 41.8979, -87.6236, "America/Chicago", 0.1075),
    ("60614", "Chicago", "Chicago", "Illinois", "IL", "United States", "US", "USD", 41.9218, -87.6481, "America/Chicago", 0.1075),
    ("60618", "Chicago", "Chicago", "Illinois", "IL", "United States", "US", "USD", 41.9468, -87.6921, "America/Chicago", 0.1075),
    ("60640", "Chicago", "Chicago", "Illinois", "IL", "United States", "US", "USD", 41.9730, -87.6582, "America/Chicago", 0.1075),
    # Pennsylvania — Philadelphia
    ("19103", "Philadelphia", "Philadelphia", "Pennsylvania", "PA", "United States", "US", "USD", 39.9526, -75.1652, "America/New_York", 0.0800),
    ("19107", "Philadelphia", "Philadelphia", "Pennsylvania", "PA", "United States", "US", "USD", 39.9481, -75.1578, "America/New_York", 0.0800),
    ("19130", "Philadelphia", "Philadelphia", "Pennsylvania", "PA", "United States", "US", "USD", 39.9648, -75.1736, "America/New_York", 0.0800),
    # Georgia — Atlanta
    ("30301", "Atlanta", "Atlanta", "Georgia", "GA", "United States", "US", "USD", 33.7490, -84.3880, "America/New_York", 0.0890),
    ("30305", "Atlanta", "Atlanta", "Georgia", "GA", "United States", "US", "USD", 33.8390, -84.3840, "America/New_York", 0.0890),
    ("30309", "Atlanta", "Atlanta", "Georgia", "GA", "United States", "US", "USD", 33.7930, -84.3830, "America/New_York", 0.0890),
    ("30318", "Atlanta", "Atlanta", "Georgia", "GA", "United States", "US", "USD", 33.7817, -84.4218, "America/New_York", 0.0890),
    # Washington — Seattle
    ("98101", "Seattle", "Seattle", "Washington", "WA", "United States", "US", "USD", 47.6062, -122.3321, "America/Los_Angeles", 0.1025),
    ("98109", "Seattle", "Seattle", "Washington", "WA", "United States", "US", "USD", 47.6340, -122.3490, "America/Los_Angeles", 0.1025),
    ("98122", "Seattle", "Seattle", "Washington", "WA", "United States", "US", "USD", 47.6091, -122.3030, "America/Los_Angeles", 0.1025),
    ("98004", "Bellevue", "Seattle", "Washington", "WA", "United States", "US", "USD", 47.6101, -122.2015, "America/Los_Angeles", 0.1025),
    # Arizona — Phoenix
    ("85001", "Phoenix", "Phoenix", "Arizona", "AZ", "United States", "US", "USD", 33.4484, -112.0740, "America/Phoenix", 0.0860),
    ("85016", "Phoenix", "Phoenix", "Arizona", "AZ", "United States", "US", "USD", 33.5094, -112.0503, "America/Phoenix", 0.0860),
    ("85251", "Scottsdale", "Phoenix", "Arizona", "AZ", "United States", "US", "USD", 33.4942, -111.9261, "America/Phoenix", 0.0860),
    ("85281", "Tempe", "Phoenix", "Arizona", "AZ", "United States", "US", "USD", 33.4255, -111.9400, "America/Phoenix", 0.0860),
    # Colorado — Denver
    ("80202", "Denver", "Denver", "Colorado", "CO", "United States", "US", "USD", 39.7392, -104.9903, "America/Denver", 0.0790),
    ("80206", "Denver", "Denver", "Colorado", "CO", "United States", "US", "USD", 39.7285, -104.9464, "America/Denver", 0.0790),
    ("80301", "Boulder", "Denver", "Colorado", "CO", "United States", "US", "USD", 40.0150, -105.2705, "America/Denver", 0.0870),
    # Massachusetts — Boston
    ("02101", "Boston", "Boston", "Massachusetts", "MA", "United States", "US", "USD", 42.3601, -71.0589, "America/New_York", 0.0625),
    ("02116", "Boston", "Boston", "Massachusetts", "MA", "United States", "US", "USD", 42.3508, -71.0757, "America/New_York", 0.0625),
    ("02134", "Boston", "Boston", "Massachusetts", "MA", "United States", "US", "USD", 42.3564, -71.1335, "America/New_York", 0.0625),
    ("02139", "Cambridge", "Boston", "Massachusetts", "MA", "United States", "US", "USD", 42.3736, -71.1097, "America/New_York", 0.0625),
    # Oregon — Portland
    ("97201", "Portland", "Portland", "Oregon", "OR", "United States", "US", "USD", 45.5051, -122.6750, "America/Los_Angeles", 0.0000),
    ("97209", "Portland", "Portland", "Oregon", "OR", "United States", "US", "USD", 45.5283, -122.6850, "America/Los_Angeles", 0.0000),
    ("97401", "Eugene", "Eugene", "Oregon", "OR", "United States", "US", "USD", 44.0521, -123.0868, "America/Los_Angeles", 0.0000),
    # Minnesota — Minneapolis
    ("55401", "Minneapolis", "Minneapolis–St. Paul", "Minnesota", "MN", "United States", "US", "USD", 44.9778, -93.2650, "America/Chicago", 0.0788),
    ("55402", "Minneapolis", "Minneapolis–St. Paul", "Minnesota", "MN", "United States", "US", "USD", 44.9767, -93.2714, "America/Chicago", 0.0788),
    ("55101", "Saint Paul", "Minneapolis–St. Paul", "Minnesota", "MN", "United States", "US", "USD", 44.9537, -93.0900, "America/Chicago", 0.0788),
    # Michigan — Detroit
    ("48201", "Detroit", "Detroit", "Michigan", "MI", "United States", "US", "USD", 42.3314, -83.0458, "America/Detroit", 0.0600),
    ("48226", "Detroit", "Detroit", "Michigan", "MI", "United States", "US", "USD", 42.3323, -83.0457, "America/Detroit", 0.0600),
    ("48104", "Ann Arbor", "Detroit", "Michigan", "MI", "United States", "US", "USD", 42.2808, -83.7430, "America/Detroit", 0.0600),
    # Ohio — Columbus / Cleveland
    ("43201", "Columbus", "Columbus", "Ohio", "OH", "United States", "US", "USD", 39.9612, -82.9988, "America/New_York", 0.0750),
    ("43215", "Columbus", "Columbus", "Ohio", "OH", "United States", "US", "USD", 39.9601, -83.0024, "America/New_York", 0.0750),
    ("44113", "Cleveland", "Cleveland", "Ohio", "OH", "United States", "US", "USD", 41.4993, -81.6944, "America/New_York", 0.0800),
    # North Carolina — Charlotte / Raleigh
    ("28202", "Charlotte", "Charlotte", "North Carolina", "NC", "United States", "US", "USD", 35.2271, -80.8431, "America/New_York", 0.0775),
    ("28205", "Charlotte", "Charlotte", "North Carolina", "NC", "United States", "US", "USD", 35.2275, -80.8080, "America/New_York", 0.0775),
    ("27601", "Raleigh", "Raleigh–Durham", "North Carolina", "NC", "United States", "US", "USD", 35.7796, -78.6382, "America/New_York", 0.0775),
    # Virginia — Northern Virginia / Richmond
    ("22201", "Arlington", "Washington D.C.", "Virginia", "VA", "United States", "US", "USD", 38.8816, -77.0910, "America/New_York", 0.0530),
    ("22301", "Alexandria", "Washington D.C.", "Virginia", "VA", "United States", "US", "USD", 38.8048, -77.0469, "America/New_York", 0.0530),
    ("23219", "Richmond", "Richmond", "Virginia", "VA", "United States", "US", "USD", 37.5407, -77.4360, "America/New_York", 0.0530),
    # Maryland — D.C. Metro
    ("20814", "Bethesda", "Washington D.C.", "Maryland", "MD", "United States", "US", "USD", 38.9807, -77.1001, "America/New_York", 0.0600),
    ("20910", "Silver Spring", "Washington D.C.", "Maryland", "MD", "United States", "US", "USD", 38.9912, -77.0261, "America/New_York", 0.0600),
    # New Jersey
    ("07030", "Hoboken", "New York City", "New Jersey", "NJ", "United States", "US", "USD", 40.7440, -74.0324, "America/New_York", 0.0663),
    ("07102", "Newark", "New York City", "New Jersey", "NJ", "United States", "US", "USD", 40.7357, -74.1724, "America/New_York", 0.0663),
    # Tennessee — Nashville
    ("37201", "Nashville", "Nashville", "Tennessee", "TN", "United States", "US", "USD", 36.1627, -86.7816, "America/Chicago", 0.0975),
    ("37205", "Nashville", "Nashville", "Tennessee", "TN", "United States", "US", "USD", 36.1374, -86.8599, "America/Chicago", 0.0975),
    # Missouri — St. Louis / Kansas City
    ("63101", "St. Louis", "St. Louis", "Missouri", "MO", "United States", "US", "USD", 38.6270, -90.1994, "America/Chicago", 0.0911),
    ("64101", "Kansas City", "Kansas City", "Missouri", "MO", "United States", "US", "USD", 39.0997, -94.5786, "America/Chicago", 0.0863),
    # Wisconsin — Milwaukee
    ("53202", "Milwaukee", "Milwaukee", "Wisconsin", "WI", "United States", "US", "USD", 43.0389, -87.9065, "America/Chicago", 0.0560),
    # South Carolina — Charleston / Greenville
    ("29401", "Charleston", "Charleston", "South Carolina", "SC", "United States", "US", "USD", 32.7765, -79.9311, "America/New_York", 0.0900),
    ("29601", "Greenville", "Greenville", "South Carolina", "SC", "United States", "US", "USD", 34.8526, -82.3940, "America/New_York", 0.0800),
    # Nevada — Las Vegas
    ("89101", "Las Vegas", "Las Vegas", "Nevada", "NV", "United States", "US", "USD", 36.1699, -115.1398, "America/Los_Angeles", 0.0838),
    ("89128", "Las Vegas", "Las Vegas", "Nevada", "NV", "United States", "US", "USD", 36.2048, -115.2386, "America/Los_Angeles", 0.0838),
    # Indiana — Indianapolis
    ("46201", "Indianapolis", "Indianapolis", "Indiana", "IN", "United States", "US", "USD", 39.7684, -86.1581, "America/Indiana/Indianapolis", 0.0700),
    # Utah — Salt Lake City
    ("84101", "Salt Lake City", "Salt Lake City", "Utah", "UT", "United States", "US", "USD", 40.7608, -111.8910, "America/Denver", 0.0720),
    ("84102", "Salt Lake City", "Salt Lake City", "Utah", "UT", "United States", "US", "USD", 40.7521, -111.8770, "America/Denver", 0.0720),
    # Kentucky — Louisville
    ("40202", "Louisville", "Louisville", "Kentucky", "KY", "United States", "US", "USD", 38.2527, -85.7585, "America/Kentucky/Louisville", 0.0600),
    # Oklahoma — Oklahoma City / Tulsa
    ("73101", "Oklahoma City", "Oklahoma City", "Oklahoma", "OK", "United States", "US", "USD", 35.4676, -97.5164, "America/Chicago", 0.0850),
    ("74101", "Tulsa", "Tulsa", "Oklahoma", "OK", "United States", "US", "USD", 36.1540, -95.9928, "America/Chicago", 0.0850),
    # Louisiana — New Orleans
    ("70112", "New Orleans", "New Orleans", "Louisiana", "LA", "United States", "US", "USD", 29.9511, -90.0715, "America/Chicago", 0.0945),
    # Kansas — Wichita
    ("67201", "Wichita", "Wichita", "Kansas", "KS", "United States", "US", "USD", 37.6872, -97.3301, "America/Chicago", 0.0750),
    # New Mexico — Albuquerque
    ("87101", "Albuquerque", "Albuquerque", "New Mexico", "NM", "United States", "US", "USD", 35.0844, -106.6504, "America/Denver", 0.0738),
    # Iowa — Des Moines
    ("50301", "Des Moines", "Des Moines", "Iowa", "IA", "United States", "US", "USD", 41.5868, -93.6250, "America/Chicago", 0.0700),
    # Nebraska — Omaha
    ("68101", "Omaha", "Omaha", "Nebraska", "NE", "United States", "US", "USD", 41.2565, -95.9345, "America/Chicago", 0.0700),
    # Idaho — Boise
    ("83701", "Boise", "Boise", "Idaho", "ID", "United States", "US", "USD", 43.6150, -116.2023, "America/Boise", 0.0600),
    # Hawaii — Honolulu
    ("96813", "Honolulu", "Honolulu", "Hawaii", "HI", "United States", "US", "USD", 21.3069, -157.8583, "Pacific/Honolulu", 0.0457),
    # Alaska — Anchorage
    ("99501", "Anchorage", "Anchorage", "Alaska", "AK", "United States", "US", "USD", 61.2181, -149.9003, "America/Anchorage", 0.0000),
    # Mississippi — Jackson
    ("39201", "Jackson", "Jackson", "Mississippi", "MS", "United States", "US", "USD", 32.2988, -90.1848, "America/Chicago", 0.0700),
    # Arkansas — Little Rock
    ("72201", "Little Rock", "Little Rock", "Arkansas", "AR", "United States", "US", "USD", 34.7465, -92.2896, "America/Chicago", 0.0950),
    # Additional secondary US cities
    ("85201", "Mesa", "Phoenix", "Arizona", "AZ", "United States", "US", "USD", 33.4152, -111.8315, "America/Phoenix", 0.0860),
    ("92618", "Irvine", "Los Angeles", "California", "CA", "United States", "US", "USD", 33.6846, -117.8265, "America/Los_Angeles", 0.0775),
    ("80112", "Centennial", "Denver", "Colorado", "CO", "United States", "US", "USD", 39.5797, -104.8772, "America/Denver", 0.0790),
    ("30062", "Marietta", "Atlanta", "Georgia", "GA", "United States", "US", "USD", 33.9526, -84.5499, "America/New_York", 0.0720),
    ("60540", "Naperville", "Chicago", "Illinois", "IL", "United States", "US", "USD", 41.7858, -88.1473, "America/Chicago", 0.0888),
    ("46032", "Carmel", "Indianapolis", "Indiana", "IN", "United States", "US", "USD", 39.9784, -86.1180, "America/Indiana/Indianapolis", 0.0700),
    ("02453", "Waltham", "Boston", "Massachusetts", "MA", "United States", "US", "USD", 42.3765, -71.2356, "America/New_York", 0.0625),
    ("48307", "Rochester Hills", "Detroit", "Michigan", "MI", "United States", "US", "USD", 42.6583, -83.1499, "America/Detroit", 0.0600),
    ("55369", "Maple Grove", "Minneapolis–St. Paul", "Minnesota", "MN", "United States", "US", "USD", 45.0724, -93.4558, "America/Chicago", 0.0788),
    ("28277", "Charlotte", "Charlotte", "North Carolina", "NC", "United States", "US", "USD", 35.0527, -80.8414, "America/New_York", 0.0775),
    ("89052", "Henderson", "Las Vegas", "Nevada", "NV", "United States", "US", "USD", 36.0395, -114.9817, "America/Los_Angeles", 0.0838),
    ("07054", "Parsippany", "New York City", "New Jersey", "NJ", "United States", "US", "USD", 40.8573, -74.4254, "America/New_York", 0.0663),
    ("14850", "Ithaca", "Ithaca", "New York", "NY", "United States", "US", "USD", 42.4440, -76.5021, "America/New_York", 0.0800),
    ("44138", "Olmsted Falls", "Cleveland", "Ohio", "OH", "United States", "US", "USD", 41.3762, -81.9048, "America/New_York", 0.0750),
    ("97330", "Corvallis", "Corvallis", "Oregon", "OR", "United States", "US", "USD", 44.5646, -123.2620, "America/Los_Angeles", 0.0000),
    ("19103", "Philadelphia", "Philadelphia", "Pennsylvania", "PA", "United States", "US", "USD", 39.9526, -75.1652, "America/New_York", 0.0800),
    ("29732", "Rock Hill", "Charlotte", "South Carolina", "SC", "United States", "US", "USD", 34.9249, -81.0251, "America/New_York", 0.0800),
    ("37211", "Nashville", "Nashville", "Tennessee", "TN", "United States", "US", "USD", 36.0892, -86.7280, "America/Chicago", 0.0975),
    ("75080", "Richardson", "Dallas–Fort Worth", "Texas", "TX", "United States", "US", "USD", 32.9483, -96.7299, "America/Chicago", 0.0825),
    ("84044", "Magna", "Salt Lake City", "Utah", "UT", "United States", "US", "USD", 40.7088, -112.1009, "America/Denver", 0.0720),
    ("22102", "McLean", "Washington D.C.", "Virginia", "VA", "United States", "US", "USD", 38.9339, -77.1773, "America/New_York", 0.0530),
    ("98033", "Kirkland", "Seattle", "Washington", "WA", "United States", "US", "USD", 47.6769, -122.2060, "America/Los_Angeles", 0.1025),
    ("53711", "Madison", "Madison", "Wisconsin", "WI", "United States", "US", "USD", 43.0731, -89.4012, "America/Chicago", 0.0560),
    # ── Canada ────────────────────────────────────────────────────────────────
    # Ontario — Toronto
    ("M5V 3A8", "Toronto", "Toronto", "Ontario", "ON", "Canada", "CA", "CAD", 43.6426, -79.3871, "America/Toronto", 0.0),
    ("M4C 1M7", "Toronto", "Toronto", "Ontario", "ON", "Canada", "CA", "CAD", 43.6890, -79.3132, "America/Toronto", 0.0),
    ("M6G 3B7", "Toronto", "Toronto", "Ontario", "ON", "Canada", "CA", "CAD", 43.6695, -79.4166, "America/Toronto", 0.0),
    ("M1B 3W3", "Scarborough", "Toronto", "Ontario", "ON", "Canada", "CA", "CAD", 43.7942, -79.2350, "America/Toronto", 0.0),
    ("L4W 4Y3", "Mississauga", "Toronto", "Ontario", "ON", "Canada", "CA", "CAD", 43.5890, -79.6441, "America/Toronto", 0.0),
    ("L6Y 4X2", "Brampton", "Toronto", "Ontario", "ON", "Canada", "CA", "CAD", 43.7315, -79.7624, "America/Toronto", 0.0),
    ("L8P 4M2", "Hamilton", "Toronto", "Ontario", "ON", "Canada", "CA", "CAD", 43.2557, -79.8711, "America/Toronto", 0.0),
    # Ontario — Ottawa
    ("K1A 0A9", "Ottawa", "Ottawa", "Ontario", "ON", "Canada", "CA", "CAD", 45.4215, -75.6972, "America/Toronto", 0.0),
    ("K2C 3K1", "Ottawa", "Ottawa", "Ontario", "ON", "Canada", "CA", "CAD", 45.3757, -75.7476, "America/Toronto", 0.0),
    # Ontario — Other
    ("N6A 3K7", "London", "London (ON)", "Ontario", "ON", "Canada", "CA", "CAD", 42.9849, -81.2453, "America/Toronto", 0.0),
    ("N2G 4Y4", "Waterloo", "Waterloo Region", "Ontario", "ON", "Canada", "CA", "CAD", 43.4668, -80.5164, "America/Toronto", 0.0),
    # Quebec — Montreal
    ("H3A 0G4", "Montreal", "Montreal", "Quebec", "QC", "Canada", "CA", "CAD", 45.5017, -73.5673, "America/Toronto", 0.0),
    ("H2X 1Z6", "Montreal", "Montreal", "Quebec", "QC", "Canada", "CA", "CAD", 45.5089, -73.5689, "America/Toronto", 0.0),
    ("H4A 1M4", "Westmount", "Montreal", "Quebec", "QC", "Canada", "CA", "CAD", 45.4890, -73.6003, "America/Toronto", 0.0),
    # Quebec — Quebec City
    ("G1R 2J5", "Quebec City", "Quebec City", "Quebec", "QC", "Canada", "CA", "CAD", 46.8139, -71.2082, "America/Toronto", 0.0),
    # British Columbia — Vancouver
    ("V6B 5K3", "Vancouver", "Vancouver", "British Columbia", "BC", "Canada", "CA", "CAD", 49.2827, -123.1207, "America/Vancouver", 0.0),
    ("V5Y 1P2", "Vancouver", "Vancouver", "British Columbia", "BC", "Canada", "CA", "CAD", 49.2636, -123.1013, "America/Vancouver", 0.0),
    ("V6H 3Y4", "Vancouver", "Vancouver", "British Columbia", "BC", "Canada", "CA", "CAD", 49.2620, -123.1432, "America/Vancouver", 0.0),
    ("V8V 1X4", "Victoria", "Victoria", "British Columbia", "BC", "Canada", "CA", "CAD", 48.4284, -123.3656, "America/Vancouver", 0.0),
    # Alberta — Calgary / Edmonton
    ("T2P 1J9", "Calgary", "Calgary", "Alberta", "AB", "Canada", "CA", "CAD", 51.0447, -114.0719, "America/Edmonton", 0.0),
    ("T2E 8Z3", "Calgary", "Calgary", "Alberta", "AB", "Canada", "CA", "CAD", 51.0775, -114.0540, "America/Edmonton", 0.0),
    ("T5J 2Z1", "Edmonton", "Edmonton", "Alberta", "AB", "Canada", "CA", "CAD", 53.5461, -113.4938, "America/Edmonton", 0.0),
    # Manitoba — Winnipeg
    ("R3C 4A5", "Winnipeg", "Winnipeg", "Manitoba", "MB", "Canada", "CA", "CAD", 49.8951, -97.1384, "America/Winnipeg", 0.0),
    # Nova Scotia — Halifax
    ("B3J 1R2", "Halifax", "Halifax", "Nova Scotia", "NS", "Canada", "CA", "CAD", 44.6488, -63.5752, "America/Halifax", 0.0),
    # Saskatchewan — Saskatoon
    ("S7K 1K4", "Saskatoon", "Saskatoon", "Saskatchewan", "SK", "Canada", "CA", "CAD", 52.1579, -106.6702, "America/Regina", 0.0),
    # ── United Kingdom ────────────────────────────────────────────────────────
    # London
    ("SW1A 1AA", "London", "London", "England", "ENG", "United Kingdom", "GB", "GBP", 51.5014, -0.1419, "Europe/London", 0.0),
    ("EC1A 1BB", "London", "London", "England", "ENG", "United Kingdom", "GB", "GBP", 51.5194, -0.0998, "Europe/London", 0.0),
    ("W1F 7JT", "London", "London", "England", "ENG", "United Kingdom", "GB", "GBP", 51.5141, -0.1367, "Europe/London", 0.0),
    ("WC2N 5DU", "London", "London", "England", "ENG", "United Kingdom", "GB", "GBP", 51.5079, -0.1268, "Europe/London", 0.0),
    ("E1 6RF", "London", "London", "England", "ENG", "United Kingdom", "GB", "GBP", 51.5131, -0.0726, "Europe/London", 0.0),
    ("SE1 7PB", "London", "London", "England", "ENG", "United Kingdom", "GB", "GBP", 51.5032, -0.1195, "Europe/London", 0.0),
    ("SW3 2AB", "London", "London", "England", "ENG", "United Kingdom", "GB", "GBP", 51.4912, -0.1678, "Europe/London", 0.0),
    # Manchester
    ("M1 1AE", "Manchester", "Manchester", "England", "ENG", "United Kingdom", "GB", "GBP", 53.4808, -2.2426, "Europe/London", 0.0),
    ("M14 5TP", "Manchester", "Manchester", "England", "ENG", "United Kingdom", "GB", "GBP", 53.4534, -2.2260, "Europe/London", 0.0),
    # Birmingham
    ("B1 1BB", "Birmingham", "Birmingham", "England", "ENG", "United Kingdom", "GB", "GBP", 52.4862, -1.8904, "Europe/London", 0.0),
    # Leeds
    ("LS1 1BA", "Leeds", "Leeds", "England", "ENG", "United Kingdom", "GB", "GBP", 53.8008, -1.5491, "Europe/London", 0.0),
    # Glasgow
    ("G1 1SB", "Glasgow", "Glasgow", "Scotland", "SCT", "United Kingdom", "GB", "GBP", 55.8642, -4.2518, "Europe/London", 0.0),
    # Edinburgh
    ("EH1 1YZ", "Edinburgh", "Edinburgh", "Scotland", "SCT", "United Kingdom", "GB", "GBP", 55.9533, -3.1883, "Europe/London", 0.0),
    # Bristol
    ("BS1 1AA", "Bristol", "Bristol", "England", "ENG", "United Kingdom", "GB", "GBP", 51.4545, -2.5879, "Europe/London", 0.0),
    # Liverpool
    ("L1 8JQ", "Liverpool", "Liverpool", "England", "ENG", "United Kingdom", "GB", "GBP", 53.4084, -2.9916, "Europe/London", 0.0),
    # ── Mexico ────────────────────────────────────────────────────────────────
    ("06600", "Mexico City", "Mexico City", "Mexico City", "CMX", "Mexico", "MX", "MXN", 19.4326, -99.1332, "America/Mexico_City", 0.0),
    ("11560", "Mexico City", "Mexico City", "Mexico City", "CMX", "Mexico", "MX", "MXN", 19.4268, -99.2019, "America/Mexico_City", 0.0),
    ("03100", "Mexico City", "Mexico City", "Mexico City", "CMX", "Mexico", "MX", "MXN", 19.3984, -99.1590, "America/Mexico_City", 0.0),
    ("44100", "Guadalajara", "Guadalajara", "Jalisco", "JAL", "Mexico", "MX", "MXN", 20.6597, -103.3496, "America/Mexico_City", 0.0),
    ("44500", "Guadalajara", "Guadalajara", "Jalisco", "JAL", "Mexico", "MX", "MXN", 20.6783, -103.4016, "America/Mexico_City", 0.0),
    ("64000", "Monterrey", "Monterrey", "Nuevo León", "NLE", "Mexico", "MX", "MXN", 25.6866, -100.3161, "America/Monterrey", 0.0),
    ("64010", "Monterrey", "Monterrey", "Nuevo León", "NLE", "Mexico", "MX", "MXN", 25.6894, -100.3054, "America/Monterrey", 0.0),
    ("22000", "Tijuana", "Tijuana", "Baja California", "BCN", "Mexico", "MX", "MXN", 32.5149, -117.0382, "America/Tijuana", 0.0),
    ("72000", "Puebla", "Puebla", "Puebla", "PUE", "Mexico", "MX", "MXN", 19.0414, -98.2063, "America/Mexico_City", 0.0),
    ("76000", "Querétaro", "Querétaro", "Querétaro", "QUE", "Mexico", "MX", "MXN", 20.5888, -100.3899, "America/Mexico_City", 0.0),
]

# Column names for GEO_RECORDS tuples
GEO_COLUMNS = [
    "postal_code", "city", "metro", "state_province", "state_code",
    "country", "country_code", "currency_code", "latitude", "longitude",
    "timezone", "tax_rate",
]

# ── Product name pools ────────────────────────────────────────────────────────

PRODUCT_ADJECTIVES: dict[str, list[str]] = {
    "tops": ["Heritage", "Essential", "Classic", "Vintage", "Weekend", "Sunday", "Studio",
             "Horizon", "Northfield", "Meridian", "Everyday", "Washed", "Garment-Dyed"],
    "bottoms": ["Straight-Leg", "Relaxed", "Slim-Fit", "Tailored", "Field", "Utility",
                "Soft", "Everyday", "Lived-In", "Classic", "Tapered"],
    "outerwear": ["Field", "Mountain", "Harbor", "Expedition", "Commuter", "Weekender",
                  "Trench", "Sherpa", "Canvas", "Storm", "Birch"],
    "footwear": ["Classic", "Heritage", "Weekend", "Trail", "Court", "Everyday",
                 "Leather", "Canvas", "Suede"],
    "accessories": ["Everyday", "Classic", "Heritage", "Woven", "Pebbled",
                    "Tumbled", "Saddle", "Mini"],
    "home": ["Stonewashed", "Woven", "Merino", "Highland", "Coastal",
             "Recycled", "Organic"],
    "beauty": ["Daily", "Essential", "Reserve", "Clean", "Botanical"],
}

PRODUCT_NOUNS: dict[str, list[str]] = {
    "tops": ["Oxford Shirt", "Chambray Tee", "Linen Shirt", "Merino Crew", "Cable Crew",
             "Waffle Henley", "Poplin Shirt", "Flannel Shirt", "Jersey Tee",
             "Pocket Tee", "Crewneck Sweatshirt", "Hoodie"],
    "bottoms": ["Chino", "Denim Jean", "Twill Pant", "Jogger", "Cord Pant",
                "Linen Pant", "Fleece Short", "Woven Short", "Canvas Short"],
    "outerwear": ["Parka", "Peacoat", "Denim Jacket", "Fleece Jacket", "Quilted Vest",
                  "Rain Jacket", "Bomber", "Corduroy Shirt Jacket", "Hoodie"],
    "footwear": ["Sneaker", "Loafer", "Boot", "Chelsea Boot", "Derby",
                 "Slip-On", "Hiker", "Moccasin"],
    "accessories": ["Tote", "Crossbody", "Backpack", "Belt", "Wallet",
                    "Card Case", "Key Fob", "Beanie", "Baseball Cap"],
    "home": ["Throw Blanket", "Candle", "Ceramic Mug", "Linen Pillow Cover",
             "Merino Blanket"],
    "beauty": ["Moisturizer", "Hand Cream", "Lip Balm", "Eau de Toilette", "Soap Bar"],
}

# ── Color pools ───────────────────────────────────────────────────────────────

COLORS: list[tuple[str, str]] = [
    # (marketing_name, color_family)
    ("Oat Heather", "neutral"),
    ("Stone", "neutral"),
    ("Canvas", "neutral"),
    ("Ivory", "neutral"),
    ("Chalk White", "neutral"),
    ("Sun Bleached", "neutral"),
    ("Bone", "neutral"),
    ("Navy", "blue"),
    ("Ink", "blue"),
    ("Cobalt", "blue"),
    ("Slate Blue", "blue"),
    ("Aegean", "blue"),
    ("Faded Denim", "blue"),
    ("Black", "black"),
    ("Washed Black", "black"),
    ("Jet", "black"),
    ("Charcoal", "grey"),
    ("Heather Grey", "grey"),
    ("Graphite", "grey"),
    ("Sage", "green"),
    ("Olive Branch", "green"),
    ("Forest", "green"),
    ("Moss", "green"),
    ("Spruce", "green"),
    ("Rust", "red"),
    ("Brick", "red"),
    ("Merlot", "red"),
    ("Terracotta", "red"),
    ("Blush", "pink"),
    ("Dusty Rose", "pink"),
    ("Camel", "brown"),
    ("Saddle", "brown"),
    ("Tobacco", "brown"),
    ("Tan", "brown"),
    ("Sea Salt", "blue"),
    ("Dusk", "purple"),
    ("Lavender", "purple"),
    ("Marigold", "yellow"),
    ("Saffron", "yellow"),
    ("Butter", "yellow"),
]

# ── Store neighborhoods (city → list of neighborhood names) ───────────────────

STORE_NEIGHBORHOODS: dict[str, list[str]] = {
    "New York": ["SoHo", "Flatiron", "Upper East Side", "West Village", "Midtown", "Nolita", "Tribeca"],
    "Los Angeles": ["West Hollywood", "Brentwood", "Silver Lake", "Los Feliz", "Venice"],
    "Chicago": ["Lincoln Park", "Gold Coast", "Wicker Park", "Bucktown", "River North"],
    "San Francisco": ["Union Square", "Hayes Valley", "Pacific Heights", "Castro", "Marina"],
    "Austin": ["Domain", "South Congress", "Barton Creek", "The Arboretum"],
    "Dallas": ["Highland Park", "Uptown", "Knox-Henderson", "Park Cities"],
    "Houston": ["Rice Village", "River Oaks", "The Heights", "Montrose", "Galleria"],
    "Seattle": ["Capitol Hill", "Queen Anne", "Fremont", "Bellevue Square", "University Village"],
    "Miami": ["Brickell", "Wynwood", "Coconut Grove", "South Beach", "Design District"],
    "Atlanta": ["Buckhead", "Inman Park", "Little Five Points", "Virginia-Highland", "Ponce City Market"],
    "Boston": ["Back Bay", "South End", "Beacon Hill", "Cambridge", "Newbury Street"],
    "Denver": ["Cherry Creek", "LoDo", "LoHi", "Congress Park", "South Pearl"],
    "Portland": ["Pearl District", "Alberta Arts District", "Hawthorne", "Division Street"],
    "Nashville": ["12 South", "The Gulch", "East Nashville", "Hillsboro Village"],
    "Charlotte": ["South End", "Plaza Midwood", "Dilworth", "Uptown"],
    "Phoenix": ["Old Town Scottsdale", "Arcadia", "Uptown", "Central Phoenix"],
    "Minneapolis": ["Uptown", "North Loop", "Linden Hills", "50th & France"],
    "Toronto": ["Yorkville", "Queen West", "King West", "Distillery District"],
    "Montreal": ["Plateau-Mont-Royal", "Old Montreal", "Outremont", "Rosemont"],
    "Vancouver": ["Gastown", "South Granville", "Kitsilano", "Yaletown"],
    "Calgary": ["Kensington", "Inglewood", "Mission", "Beltline"],
    "London": ["Covent Garden", "Marylebone", "Chelsea", "Notting Hill", "Shoreditch", "Canary Wharf"],
    "Manchester": ["Northern Quarter", "Spinningfields", "Didsbury"],
    "Birmingham": ["Brindleyplace", "Harborne", "Moseley"],
    "Leeds": ["Headingley", "Chapel Allerton", "City Centre"],
    "Glasgow": ["West End", "Merchant City", "Finnieston"],
    "Edinburgh": ["New Town", "Stockbridge", "Leith"],
    "Mexico City": ["Polanco", "Condesa", "Roma Norte", "Santa Fe"],
    "Guadalajara": ["Providencia", "Chapalita", "Tlaquepaque"],
    "Monterrey": ["San Pedro Garza García", "Valle", "Cumbres"],
    "Tijuana": ["Zona Río", "Playas", "Garita"],
}

# ── Region managers (for dim_store) ──────────────────────────────────────────

REGION_MANAGERS: list[str] = [
    "Sarah Chen", "Marcus Johnson", "Priya Patel", "James Rodriguez",
    "Emily Thompson", "David Kim", "Rachel Martinez", "Michael O'Brien",
    "Natalie Williams", "Ryan Nakamura", "Aisha Washington", "Tyler Brooks",
    "Olivia Fernandez", "Connor Walsh", "Destiny Jackson",
]

DISTRICTS: dict[str, list[str]] = {
    "US": [
        "Northeast", "Mid-Atlantic", "Southeast", "Florida",
        "Great Lakes", "Midwest", "South Central", "Texas",
        "Mountain West", "Pacific Northwest", "California North", "California South",
        "Desert Southwest", "Hawaii",
    ],
    "CA": ["Canada East", "Canada West", "Canada Central"],
    "GB": ["UK"],
    "MX": ["Mexico"],
}

CLIMATE_ZONES: dict[str, str] = {
    # US state → climate zone
    "AK": "Cold", "ME": "Cold", "VT": "Cold", "NH": "Cold", "MN": "Cold",
    "WI": "Cold", "MI": "Cold", "NY": "Cold", "MA": "Cold", "RI": "Cold",
    "CT": "Cold", "PA": "Cold", "OH": "Cold", "IN": "Cold", "IL": "Cold",
    "IA": "Cold", "MO": "Temperate", "NE": "Temperate", "KS": "Temperate",
    "CO": "Cold", "UT": "Cold", "ID": "Cold", "WY": "Cold", "MT": "Cold",
    "ND": "Cold", "SD": "Cold",
    "WA": "Temperate", "OR": "Temperate", "NV": "Warm", "CA": "Warm",
    "AZ": "Hot", "NM": "Warm", "TX": "Hot", "OK": "Warm", "AR": "Warm",
    "LA": "Hot", "MS": "Hot", "AL": "Hot", "GA": "Warm", "SC": "Warm",
    "NC": "Warm", "VA": "Temperate", "WV": "Temperate", "KY": "Temperate",
    "TN": "Warm", "FL": "Hot", "MD": "Temperate", "DE": "Temperate",
    "NJ": "Temperate", "HI": "Hot",
    # Canadian provinces
    "ON": "Cold", "QC": "Cold", "BC": "Temperate", "AB": "Cold",
    "MB": "Cold", "SK": "Cold", "NS": "Cold",
    # UK / MX
    "ENG": "Temperate", "SCT": "Cold",
    "CMX": "Warm", "JAL": "Warm", "NLE": "Hot", "BCN": "Warm",
    "PUE": "Warm", "QUE": "Warm",
}
