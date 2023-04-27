"""Module for the main function
"""

from src.spotify import Spotify


try:
    Spotify().execute()

except ValueError as value_err:
    print(value_err)

except ConnectionError as connection_err:
    print(connection_err)
