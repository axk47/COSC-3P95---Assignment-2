from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
    ConsoleMetricExporter,
)

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

from opentelemetry.sdk.trace.sampling import TraceIdRatioBased



def _make_sampler(sampling: str):
    """
    sampling:
      - "always_on" -> equivalent to TraceIdRatioBased(1.0)
      - any string that can be parsed as float between 0 and 1 -> TraceIdRatioBased
        e.g. "0.2" for 20% sampling
    """
    if sampling == "always_on":
        return TraceIdRatioBased(1.0)

    try:
        ratio = float(sampling)
        if 0.0 <= ratio <= 1.0:
            return TraceIdRatioBased(ratio)
    except ValueError:
        pass

    # fallback: full sampling
    return TraceIdRatioBased(1.0)

from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

def _make_sampler(sampling: str):
    """
    Return a TraceIdRatioBased sampler based on the sampling argument.

    sampling:
      - "always_on"  -> 100% of traces
      - "0.2"        -> 20% of traces
      - "0.1"        -> 10%, etc.
    """
    if sampling == "always_on":
        return TraceIdRatioBased(1.0)

    # try to interpret as a float probability between 0 and 1
    try:
        p = float(sampling)
        if 0.0 <= p <= 1.0:
            return TraceIdRatioBased(p)
    except ValueError:
        pass

    # fallback: be safe and keep everything
    return TraceIdRatioBased(1.0)


def setup_tracing(service_name: str, sampling: str = "always_on"):
    """
    Configure a TracerProvider with an OTLP exporter and a configurable sampler.

    sampling:
      - "always_on"
      - e.g. "0.2" for 20% trace sampling
    """
    resource = Resource.create({"service.name": service_name})

    sampler = _make_sampler(sampling)
    provider = TracerProvider(resource=resource, sampler=sampler)

    span_exporter = OTLPSpanExporter(
        endpoint="localhost:4317",
        insecure=True,
    )
    span_processor = BatchSpanProcessor(span_exporter)
    provider.add_span_processor(span_processor)

    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(service_name)
    return tracer



def setup_metrics(service_name: str):
    resource = Resource.create({"service.name": service_name})

    metric_exporter = OTLPMetricExporter(
        endpoint="localhost:4317",
        insecure=True,
    )

    reader = PeriodicExportingMetricReader(metric_exporter)

    provider = MeterProvider(
        resource=resource,
        metric_readers=[reader],
    )

    metrics.set_meter_provider(provider)
    return metrics.get_meter(service_name)


# Use below funtion if you want to see the histogram metric and counter metric in your server and client terminal 
# and comment out the above function
'''
def setup_metrics(service_name: str):
    """
    Configure a MeterProvider with:
      - OTLP metric exporter (to the collector)
      - Console metric exporter (for screenshots / inspection)
    """
    resource = Resource.create({"service.name": service_name})

    # ---- OTLP exporter (same as before) ----
    otlp_exporter = OTLPMetricExporter(
        endpoint="localhost:4317",
        insecure=True,
    )
    otlp_reader = PeriodicExportingMetricReader(otlp_exporter)

    # ---- Console exporter (NEW) ----
    console_exporter = ConsoleMetricExporter()
    console_reader = PeriodicExportingMetricReader(
        console_exporter,
        export_interval_millis=5000,  # export every 5 seconds
    )

    # Attach BOTH readers to the provider
    provider = MeterProvider(
        resource=resource,
        metric_readers=[otlp_reader, console_reader],
    )

    metrics.set_meter_provider(provider)
    meter = metrics.get_meter(service_name)
    return meter
'''
