using System;
using System.Threading;
using Confluent.Kafka;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace ConsumoKafka
{
    class Program
    {
        static void Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .CreateLogger();
            logger.Information("Testando o consumo de mensagens com Kafka");

            if (args.Length != 3)
            {
                logger.Error(
                    "Informe 3 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic a ser utilizado no consumo das mensagens, " +
                    "no terceiro o Group Id da aplicação...");
                return;
            }

            string bootstrapServers = args[0];
            string nomeTopic = args[1];
            string groupId = args[2];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");
            logger.Information($"Group Id = {groupId}");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(nomeTopic);

                    try
                    {
                        while (true)
                        {
                            var cr = consumer.Consume(cts.Token);
                            logger.Information(
                                $"Mensagem lida: {cr.Message.Value}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                        logger.Warning("Cancelada a execução do Consumer...");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}