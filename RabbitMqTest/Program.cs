using NLog;
using System;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using RabbitMqTest.SimpleHelpers;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace RabbitMqTest
{
	class Program
	{
		public static FlexibleOptions ProgramOptions { get; private set; }

		/// <summary>
		/// Main program entry point.
		/// </summary>
		static void Main (string[] args)
		{
			// set error exit code
			System.Environment.ExitCode = -50;
			try
			{
				// load configurations
				ProgramOptions = ConsoleUtils.Initialize (args, true);           
					
				CancellationTokenSource cts = new CancellationTokenSource ();

				System.Console.CancelKeyPress += (s, e) =>
				{
					logger.Debug ("User requested exit (Ctrl + C), exiting...");
					e.Cancel = true;
					cts.Cancel ();
				};

				// start execution
				ExecuteAsync (ProgramOptions, cts.Token).Wait ();

				// check before ending for waitForKeyBeforeExit option
				if (ProgramOptions.Get ("waitForKeyBeforeExit", false))
					ConsoleUtils.WaitForAnyKey ();
			}
			catch (Exception ex)
			{
				LogManager.GetCurrentClassLogger ().Fatal (ex);

				// check before ending for waitForKeyBeforeExit option
				if (ProgramOptions != null && ProgramOptions.Get ("waitForKeyBeforeExit", false))
					ConsoleUtils.WaitForAnyKey ();

				ConsoleUtils.CloseApplication (-60, true);
			}
			// set success exit code
			ConsoleUtils.CloseApplication (0, false);
		}
		
		static Logger logger = LogManager.GetCurrentClassLogger ();
		static DateTime started = DateTime.UtcNow;

		private static async Task ExecuteAsync (FlexibleOptions options, CancellationToken token)
		{
			logger.Info ("Start");
		   
			// TODO: implement execution
			// ...
			// if (token.IsCancellationRequested) return;
			// ...
			
			var publish = Task.Run (() => {
				using (var queue = CreateQueue ())
				{
					for (var j = 0; j < 1000; j++)
					{ 
						for (var i = 0; i < 250; i++)
                            queue.Publish ("teste khalid " + i);
						logger.Debug ("publish progress " + (j + 1) * 250);
					}
				}
			});

            //Task.Delay (30000).Wait ();

			var consume = Task.Run (() =>
			{
				int count = 0;
				using (var queue = CreateQueue ())
				{                    
                    foreach (var i in queue.Get (TimeSpan.FromSeconds (1800)))                                        
					//ParallelTasks<RabbitWorkMessage>.Process (queue.Get (TimeSpan.FromSeconds (1800), true), 4, i =>
                    {
                        // ... 
                        //i.Ack ();

                        if (count % 2 == 0)
                            i.Nack ();
                        else
                            i.Requeue ();
                        
                        //Task.Delay (50).Wait ();
                        if (count++ % 250 == 0)
                            logger.Debug ("ack progress " + count);
                    }
                    //});
				}
			});

			await publish;
			consume.Wait ();
			
			logger.Info ("End");
		}

		public static RabbitWorkQueue CreateQueue ()
		{
            return new RabbitWorkQueue (ProgramOptions.Get ("rabbitmq"), "RabbitMqTest-test-queue", 0, RabbitWorkQueueMode.Transient, 0, 1000);
		}

	}
}