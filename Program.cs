using System;
using MPI;
using Microsoft.Data.SqlClient;
using System.Diagnostics;

class Program
{
    static void Main(string[] args)
    {

        MPI.Environment.Run(ref args, comm =>
        {
            var con = new SqlConnection(@"Data Source=DESKTOP-FPCMKB1; Initial Catalog=TSQL2012; Integrated Security=true;TrustServerCertificate=True");
            con.Open();

            if (comm.Rank == 0)
            {
                var sw = new Stopwatch();
                sw.Start();

                for (int dest = 1; dest < comm.Size; ++dest)
                {
                    comm.Send("Start", dest, 0);
                    string res = comm.Receive<string>(dest, 1);
                    Console.WriteLine(res);
                }

                sw.Stop();
                Console.Write($"Время выполнения: {sw.Elapsed.Milliseconds} мс");
            }
            else if(comm.Rank == 1)
            {
                comm.Receive<string>(0, 0);
                string sql = "SELECT categoryname FROM Production.Categories";
                using var cmd = new SqlCommand(sql, con);

                using SqlDataReader rdr = cmd.ExecuteReader();
                var str = "";

                while (rdr.Read())
                {
                    str += rdr.GetString(0) + "\n";
                                
                }
                comm.Send(str, 0, 1);
            }
            else if (comm.Rank == 2)
            {
                comm.Receive<string>(0, 0);
                string sql = "SELECT productname FROM Production.Products";
                using var cmd = new SqlCommand(sql, con);

                using SqlDataReader rdr = cmd.ExecuteReader();
                var str = "";

                while (rdr.Read())
                {
                   str += rdr.GetString(0) + "\n";
                    
                }
                comm.Send(str, 0, 1);
            }
            else if (comm.Rank == 3)
            {
                comm.Receive<string>(0, 0);
                string sql = "SELECT address FROM Production.Suppliers";
                using var cmd = new SqlCommand(sql, con);

                using SqlDataReader rdr = cmd.ExecuteReader();
                var str = "";

                while (rdr.Read())
                {
                   str += rdr.GetString(0) + "\n";
                    
                }
                comm.Send(str, 0, 1);
            }
        });
    }
}
