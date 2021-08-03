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
            var con = "Host=localhost;Username=postgres;Password=sps@7890;Database=school;";
            using var conn = new Npgsql.NpgsqlConnection(con);
            conn.Open();

            ConcurrentDictionary<int, TripDetails> tripDict = new ConcurrentDictionary<int, TripDetails>();
            Console.WriteLine("HI");
           
         
          BrokerConfig();

            async Task BrokerConfig()
            {
                conn.Notification += (o, e) =>
                
                {
                    
                    
                    Console.WriteLine(e.Payload);

                    Trip_Demo data = JsonConvert.DeserializeObject<Trip_Demo>(e.Payload);

                    Console.WriteLine(data.table);

                    var dictData = JsonConvert.SerializeObject(data.row);

                    string operationType = data.type;
                    TripDetails td = JsonConvert.DeserializeObject<TripDetails>(dictData);

                    if (operationType.Equals("INSERT") || operationType.Equals("UPDATE"))
                    {
                        tripDict.AddOrUpdate(td.trip_id,td,(key,oldvalue)=>td);

                    }

                    else if (operationType.Equals("DELETE"))
                    {
                        TripDetails incident;
                        tripDict.TryRemove(td.trip_id, out incident);
                    }

                    foreach (var i in tripDict)
                    {
                        Console.WriteLine("Key:{0}"+" "+"Value:{1}",i.Key,i.Value.trip_name);

                    }
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
