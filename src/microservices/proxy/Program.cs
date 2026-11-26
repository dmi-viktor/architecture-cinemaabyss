using System.Text;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls($"http://*:{Environment.GetEnvironmentVariable("PORT") ?? "8000"}");

var monolithUrl       = Environment.GetEnvironmentVariable("MONOLITH_URL")       ?? "http://monolith:8080";
var moviesServiceUrl  = Environment.GetEnvironmentVariable("MOVIES_SERVICE_URL")  ?? "http://movies-service:8081";
var eventsServiceUrl  = Environment.GetEnvironmentVariable("EVENTS_SERVICE_URL")  ?? "http://events-service:8082";

var gradualMigration = string.Equals(
    Environment.GetEnvironmentVariable("GRADUAL_MIGRATION"),
    "true", StringComparison.OrdinalIgnoreCase);

var migrationPercent = 0;
var percentRaw = Environment.GetEnvironmentVariable("MOVIES_MIGRATION_PERCENT");
if (!int.TryParse(percentRaw, out migrationPercent) || migrationPercent is < 0 or > 100)
{
    Console.WriteLine($"[warn] Некорректный MOVIES_MIGRATION_PERCENT=\"{percentRaw}\", использую 0");
    migrationPercent = 0;
}

Console.WriteLine(
    $"[info] Proxy started. Monolith={monolithUrl}, Movies={moviesServiceUrl}, Events={eventsServiceUrl}, " +
    $"GradualMigration={gradualMigration}, MoviesMigrationPercent={migrationPercent}");

var httpClient = new HttpClient(new SocketsHttpHandler
{
    AllowAutoRedirect = false,
    ConnectTimeout = TimeSpan.FromSeconds(5)
});

var hopByHopHeaders = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
{
    "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
    "Te", "Trailer", "Transfer-Encoding", "Upgrade"
};

async Task ForwardAsync(HttpContext ctx, string targetBase)
{
    var target = new Uri(targetBase);
    var requestUri = new Uri(target, ctx.Request.Path + ctx.Request.QueryString.Value);

    using var upstream = new HttpRequestMessage(new HttpMethod(ctx.Request.Method), requestUri);

    foreach (var (name, values) in ctx.Request.Headers)
    {
        if (hopByHopHeaders.Contains(name) || name.Equals("Host", StringComparison.OrdinalIgnoreCase))
            continue;

        if (!upstream.Headers.TryAddWithoutValidation(name, values.ToArray()))
        {
            upstream.Content ??= new StreamContent(ctx.Request.Body);
            upstream.Content.Headers.TryAddWithoutValidation(name, values.ToArray());
        }
    }

    if (upstream.Content is null &&
        (ctx.Request.ContentLength is > 0 || ctx.Request.Headers.ContainsKey("Transfer-Encoding")))
    {
        upstream.Content = new StreamContent(ctx.Request.Body);
    }

    try
    {
        using var downstream = await httpClient.SendAsync(
            upstream, HttpCompletionOption.ResponseHeadersRead, ctx.RequestAborted);

        ctx.Response.StatusCode = (int)downstream.StatusCode;

        foreach (var (name, values) in downstream.Headers)
            if (!hopByHopHeaders.Contains(name))
                ctx.Response.Headers[name] = values.ToArray();

        foreach (var (name, values) in downstream.Content.Headers)
            if (!hopByHopHeaders.Contains(name) && name != "Content-Length")
                ctx.Response.Headers[name] = values.ToArray();

        await downstream.Content.CopyToAsync(ctx.Response.Body, ctx.RequestAborted);
    }
    catch (HttpRequestException ex)
    {
        ctx.Response.StatusCode = StatusCodes.Status502BadGateway;
        await ctx.Response.WriteAsJsonAsync(new { error = $"Upstream unavailable: {ex.Message}" }, ctx.RequestAborted);
    }
}

string GetDistributionKey(HttpContext ctx)
{
    if (ctx.Request.Headers.TryGetValue("X-User-Id", out var userId) && !string.IsNullOrEmpty(userId))
        return $"user:{userId}";

    var auth = ctx.Request.Headers.Authorization.ToString();
    if (!string.IsNullOrEmpty(auth))
        return $"token:{auth}";

    return $"ip:{ctx.Connection.RemoteIpAddress}";
}

uint ComputeHash(string value)
{
    unchecked
    {
        uint hash = 2166136261;
        foreach (var b in Encoding.UTF8.GetBytes(value))
        {
            hash ^= b;
            hash *= 16777619;
        }
        return hash;
    }
}

bool ShouldUseMoviesService(HttpContext ctx)
{
    if (!gradualMigration || migrationPercent <= 0) return false;
    if (migrationPercent >= 100) return true;

    return ComputeHash(GetDistributionKey(ctx)) % 100 < (uint)migrationPercent;
}

var app = builder.Build();

app.Run(async ctx =>
{
    if (HttpMethods.IsGet(ctx.Request.Method) && ctx.Request.Path == "/health")
    {
        ctx.Response.StatusCode = StatusCodes.Status200OK;
        await ctx.Response.WriteAsJsonAsync(new { status = "ok" }, ctx.RequestAborted);
        return;
    }

    if (ctx.Request.Path.StartsWithSegments("/api/movies"))
    {
        await ForwardAsync(ctx, ShouldUseMoviesService(ctx) ? moviesServiceUrl : monolithUrl);
        return;
    }

    if (ctx.Request.Path.StartsWithSegments("/api/events"))
    {
        await ForwardAsync(ctx, monolithUrl);
        return;
    }

    await ForwardAsync(ctx, monolithUrl);
});

app.Run();
