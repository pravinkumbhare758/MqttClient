using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using HealthChecks.UI.Client;
using MqttClient.Configuration;
using MqttClient.Handlers;
using MqttClient.HealthChecks;
using MqttClient.Interfaces;
using MqttClient.Metrics;
using MqttClient.Services;
using MqttClient.Tracing;

var builder = WebApplication.CreateBuilder(args);

// ── Configuration (Step 7) ───────────────────────────────────────────────────
builder.Services.Configure<MqttOptions>(builder.Configuration.GetSection(MqttOptions.Section));
builder.Services.Configure<WorkerPoolOptions>(builder.Configuration.GetSection(WorkerPoolOptions.Section));

// ── Message Handlers (Step 9 – pre-compiled router) ─────────────────────────
builder.Services.AddSingleton<IMqttMessageHandler, TelemetryHandler>();
builder.Services.AddSingleton<IMqttMessageHandler, CommandHandler>();
builder.Services.AddSingleton<ITopicRouter, TopicRouter>();

// ── Core services ────────────────────────────────────────────────────────────
builder.Services.AddSingleton<DeadLetterService>();
builder.Services.AddSingleton<MqttConnectionState>();
builder.Services.AddSingleton<MqttMetrics>();
builder.Services.AddHostedService<MqttConsumerService>();

// ── OpenTelemetry – Metrics & Traces (Steps 3 & 10) ─────────────────────────
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(MqttMetrics.MeterName)
            .AddRuntimeInstrumentation()
            .AddPrometheusExporter();
    })
    .WithTracing(tracing =>
    {
        tracing
            .AddSource(MqttActivitySource.Source.Name)
            .AddOtlpExporter();   // configure OTEL_EXPORTER_OTLP_ENDPOINT env var
    });

// ── Health Checks (Step 3) ───────────────────────────────────────────────────
builder.Services.AddHealthChecks()
    .AddCheck<MqttHealthCheck>(
        "mqtt",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["live", "ready"]);

var app = builder.Build();

// ── Prometheus scrape endpoint ────────────────────────────────────────────────
app.MapPrometheusScrapingEndpoint("/metrics");

// ── Health endpoints ──────────────────────────────────────────────────────────
app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate      = hc => hc.Tags.Contains("live"),
    ResponseWriter = UIResponseWriter.WriteHealthCheckUIResponse
});
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate      = hc => hc.Tags.Contains("ready"),
    ResponseWriter = UIResponseWriter.WriteHealthCheckUIResponse
});

await app.RunAsync();
