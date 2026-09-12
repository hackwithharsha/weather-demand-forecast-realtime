"""
Stream feature consumer package.

Exports the two consumer-loop functions started by ``app.main``.
"""

from .consumer import demand_stream_consumer_loop, weather_stream_consumer_loop

__all__ = ["demand_stream_consumer_loop", "weather_stream_consumer_loop"]
