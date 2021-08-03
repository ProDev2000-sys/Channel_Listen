using Npgsql;
using System;
using System.Collections.Concurrent;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Data;

namespace ListeningFromChannel
{
    class Program
    {
        static void Main(string[] args)
        {
            var con = "Host=localhost;Username=postgres;Password=prats212;Database=school;";
            using var conn = new Npgsql.NpgsqlConnection(con);
            conn.Open();
         
          BrokerConfig();

            async Task BrokerConfig()
            {
                conn.Notification += (o, e) =>
                
                {
                    
                    Console.WriteLine(e.Payload);

                    Trip_Demo data = JsonConvert.DeserializeObject<Trip_Demo>(e.Payload);

                    Console.WriteLine(data.table);
              
                };
                await using (var cmd = new NpgsqlCommand())
                {
                    cmd.CommandText = "LISTEN trip_demo_channel";
                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = conn;
                    cmd.ExecuteNonQuery();
                }

                while (true)
                {
                    // Waiting for Event
                    conn.Wait();
                }
            }


           
           

        }
    }
}
