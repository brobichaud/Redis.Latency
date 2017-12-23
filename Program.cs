using System;
using System.IO;
using System.Linq;
using System.Threading;
using CsvHelper;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using StackExchange.Redis;

namespace RedisLatencyChecker
{
	internal class Program
	{
		static readonly ILog Log = LogManager.GetLogger(typeof(Program));

		static void Main(string[] args)
		{
			ConfigureLogging();

			Log.Info("Starting Up");

			// Create Servers
			var servers = args.Select(x => new Server(x)).ToArray();

			// Sanity Check
			if (servers.Length == 0)
			{
				Log.Error("Usage: RedisLatencyChecker <\"ConnectionString1\" [\"ConnectionString2\" ...]>");
				return;
			}

			// Print Diagnostics
			foreach (var server in servers)
			{
				var serverName = server.ConnString.Split('.').First();
				Log.Info(string.Format("Monitoring: {0}", serverName));
			}

			// Start Latency Checking
			const string subDirectory = "Results";
			Directory.CreateDirectory(subDirectory);
			var filename = string.Format("{0}/{1}.csv", subDirectory, DateTime.Now.ToString("yyyy-MM-dd_HHmmss"));
			using (var streamWriter = new StreamWriter(filename))
			{
				streamWriter.AutoFlush = true;

				using (var writer = new CsvWriter(streamWriter))
				{
					// Header
					writer.WriteField("TimeUtc");
					for (var i = 0; i < servers.Length; i++)
					{
						writer.WriteField(string.Format("Server{0}.Problem", i));
						writer.WriteField(string.Format("Server{0}.Ping", i));
						writer.WriteField(string.Format("Server{0}.Error", i));
					}
					writer.NextRecord();

					// Ping Servers
					while (true)
					{
						writer.WriteField(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss"));

						foreach (var server in servers)
						{
							var serverName = server.ConnString.Split('.').First();

							// Ping Server
							Exception exception = null;
							TimeSpan? elapsed = null;
							try
							{
								elapsed = server.Multiplexer.GetDatabase().Ping();
								Log.Info(string.Format("Ping OK: {0}, {1}ms", serverName,
									elapsed.Value.TotalMilliseconds));
							}
							catch (Exception ex)
							{
								exception = ex;
								Log.Error(string.Format("Ping Error: {0}, {1}", serverName, exception));
							}

							// Write
							writer.WriteField(exception != null ? 1 : 0);
							writer.WriteField(elapsed != null ? (double?)elapsed.Value.TotalMilliseconds : null);
							writer.WriteField(exception != null ? exception.ToString() : null);
						}

						// Flush
						writer.NextRecord();

						// Wait
						Thread.Sleep(5 * 1000);
					}
				}
			}
		}

		static void ConfigureLogging()
		{
			var consoleAppender = new ConsoleAppender();
			consoleAppender.Layout = new PatternLayout("[%date{yyyy-MM-dd HH:mm:ss}] %-5p %c{1} - %m%n");

			BasicConfigurator.Configure(consoleAppender);
		}

		class Server
		{
			public Server(string connString)
			{
				ConnString = connString;
				Logger = new StringWriter();

				var options = ConfigurationOptions.Parse(connString);
				Multiplexer = ConnectionMultiplexer.Connect(options, Logger);
			}

			public StringWriter Logger { get; set; }

			public string ConnString { get; set; }
			public ConnectionMultiplexer Multiplexer { get; set; }
		}
	}
}