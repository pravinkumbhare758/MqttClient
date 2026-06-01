using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
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

// ── Configuration with startup validation ────────────────────────────────────
builder.Services
    .AddOptions<MqttOptions>()
    .BindConfiguration(MqttOptions.Section)
    .ValidateOnStart();

builder.Services
    .AddOptions<WorkerPoolOptions>()
    .BindConfiguration(WorkerPoolOptions.Section)
    .ValidateOnStart();

// IValidateOptions implementations — called by ValidateOnStart above
builder.Services.AddSingleton<IValidateOptions<MqttOptions>, MqttOptionsValidator>();
builder.Services.AddSingleton<IValidateOptions<WorkerPoolOptions>, WorkerPoolOptionsValidator>();

// ── Message Handlers ──────────────────────────────────────────────────────────
// Register all IMqttMessageHandler implementations before TopicRouter so the
// IEnumerable<IMqttMessageHandler> constructor parameter is populated.
builder.Services.AddSingleton<IMqttMessageHandler, TelemetryHandler>();
builder.Services.AddSingleton<IMqttMessageHandler, CommandHandler>();
builder.Services.AddSingleton<ITopicRouter, TopicRouter>();

// ── Core services ─────────────────────────────────────────────────────────────
builder.Services.AddSingleton<DeadLetterService>();
builder.Services.AddSingleton<MqttConnectionState>();
builder.Services.AddSingleton<MqttMetrics>();
builder.Services.AddHostedService<MqttConsumerService>();

// ── OpenTelemetry — Metrics & Traces ─────────────────────────────────────────
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(MqttMetrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter())
    .WithTracing(tracing => tracing
        .AddSource(MqttActivitySource.Source.Name)
        .AddOtlpExporter());   // set OTEL_EXPORTER_OTLP_ENDPOINT env var

// ── Health checks ─────────────────────────────────────────────────────────────
builder.Services.AddHealthChecks()
    .AddCheck<MqttHealthCheck>(
        "mqtt",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["live", "ready"]);

var app = builder.Build();

app.MapPrometheusScrapingEndpoint("/metrics");

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
