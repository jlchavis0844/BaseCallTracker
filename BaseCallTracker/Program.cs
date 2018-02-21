using System;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Newtonsoft.Json.Linq;
using unirest_net.http;
using System.Data.SqlClient;
using System.Data;

namespace BaseCallTracker {
    class Program {
        public static Dictionary<int, string> ownersData = new Dictionary<int, string>();
        public static Dictionary<int, string> outcomeData = new Dictionary<int, string>();
        public static List<Owner> owners;
        public static List<Call> conList;
        public static StreamWriter log;
        public static string position = "";
        public static string[] resTypes = { "lead", "contact" };
        public static string[] ocTypes = { "leadToCon", "other" };
        public static int[] conToLeadOutcomes = { 1283017, 1341185, 1350524, 1394566 };
        public static string token = "";
        public static Random random = new Random();
        public static string connString = "Data Source=RALIMSQL1;Initial Catalog=CAMSRALFG;Integrated Security=SSPI;";
        public static DateTime limit;
        public static string line = @"INSERT INTO [CAMSRALFG].[dbo].[Base_Calls] ([id], [user_id], [name], [outcome_id], [outcome], [duration], " +
                                    "[phone_number], [incoming], [missed], [updated_at], [made_at], [resource_id], [resource_type], [prevRType], " +
                                    "[prevRID], [position], [event_type]) VALUES (@id, @user_id, @name, @outcome_id, @outcome, @duration, @phone_number, " +
                                    "@incoming, @missed, @updated_at, @made_at, @resource_id, @resource_type, @prevRType, @prevRID, @position, @event_type);";

        static void Main(string[] args) {
            string startURL = @"https://api.getbase.com/v3/calls/stream?limit=100&position=";
            DateTime now = DateTime.Now;
            var fs = new FileStream(@"C:\apps\NiceOffice\token", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var ps = new FileStream(@"\\NAS3\NOE_Docs$\RALIM\Logs\Base\CallPosition", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            using (var tokenReader = new StreamReader(fs)) {
                token = tokenReader.ReadToEnd();
            }

            using (var posReader = new StreamReader(ps)) {
                position = posReader.ReadToEnd();
            }


            string logPath = @"\\NAS3\NOE_Docs$\RALIM\Logs\Base\CallTracker_" + now.ToString("ddMMyyyy") + ".txt";
            if (!File.Exists(logPath)) {
                using (StreamWriter sw = File.CreateText(logPath)) {
                    sw.WriteLine("Creating call check log file for " + now.ToString("ddMMyyyy") + " at " + now.ToString());
                }
            }

            log = File.AppendText(logPath);
            log.WriteLine("\n\nStarting call check at " + now);
            Console.WriteLine("Starting call check at " + now);

            owners = new List<Owner>();
            conList = new List<Call>();

            SetOwnerData();
            SetOutcomeData();
            bool top = false;

            while (!top) {
                string rawData = Get(startURL + position, token);
                var jsonObj = JObject.Parse(rawData) as JObject;
                var items = jsonObj["items"] as JArray;
                top = Convert.ToBoolean(jsonObj["meta"]["top"]);

                log.WriteLine("starting check of " + items.Count + " events at position " + position);
                Console.WriteLine("starting check of " + items.Count + " events at position " + position);

                foreach (var item in items) {
                    var data = item["data"];
                    var meta = item["meta"];
                    
                    if(!meta.HasValues || meta["event_type"] == null || meta["event_type"].ToString() == "deleted") {
                        continue;
                    }

                    int user_id = Convert.ToInt32(data["user_id"]);
                    if (!ownersData.ContainsKey(user_id)) {
                        continue;
                    }

                    bool missed = true;
                    if (data["missed"] == null && data["duration"] != null &&
                        Convert.ToInt32(data["duration"]) > 0) {
                        missed = false;
                    } else if (data["missed"] != null && data["missed"].ToString() != "") {
                        missed = Convert.ToBoolean(data["missed"]);
                    }

                    if (missed) {
                        continue;
                    }

                    int id = Convert.ToInt32(data["id"]);
                    DateTime made_at = Convert.ToDateTime(data["made_at"]).ToLocalTime();
                    DateTime updated_at = Convert.ToDateTime(meta["event_time"]).ToLocalTime();
                    string name = ownersData[user_id];
                    
                    string resource_type = "unknown";
                    if (data["resource_type"] != null && data["resource_type"].ToString() != "") {
                        resource_type = data["resource_type"].ToString();
                    }

                    int outcome_id = 0;
                    if (data["outcome_id"] != null && data["outcome_id"].ToString() != "") {
                        outcome_id = Convert.ToInt32(data["outcome_id"]);
                    }

                    string outcome = "Unknown";
                    if (outcomeData.ContainsKey(outcome_id)) {
                        outcome = outcomeData[outcome_id];
                    }

                    int duration = 0;
                    if (data["duration"] != null && data["duration"].ToString() != "") {
                        duration = Convert.ToInt32(data["duration"]);
                    }

                    bool incoming = Convert.ToBoolean(data["incoming"]);
                    string phone_number = data["phone_number"].ToString();

                    int resource_id = 0;
                    if (data["resource_id"] != null && data["resource_id"].ToString() != "") {
                        resource_id = Convert.ToInt32(data["resource_id"]);
                    }
                    string event_type = "Unknown";
                    if (meta["event_type"] != null && meta["event_type"].ToString() != "") {
                        event_type = meta["event_type"].ToString();
                    }

                    string prevRType = "Unknown";
                    if(event_type != "created" && meta["previous"]["resource_type"] != null && meta["previous"]["resource_type"].ToString() != "") {
                        prevRType = meta["previous"]["resource_type"].ToString();
                    }

                    int prevRID = 0;
                    if (event_type != "created" && meta["previous"]["resource_id"] != null && meta["previous"]["resource_id"].ToString() != "") {
                        prevRID = Convert.ToInt32(meta["previous"]["resource_id"]);
                    }

                    if(event_type == "updated" && prevRType == "Unknown" && prevRID == 0) {
                        continue; //gets rid of unknown to contact line
                    }

                    Call tCall = new Call();
                    tCall.id = id;
                    tCall.user_id = user_id;
                    tCall.made_at = made_at;
                    tCall.updated_at = updated_at;
                    tCall.duration = duration;
                    tCall.name = name;
                    tCall.resource_id = resource_id;
                    tCall.resource_type = resource_type;
                    tCall.outcome_id = outcome_id;
                    tCall.outcome = outcome;
                    tCall.missed = missed;
                    tCall.incoming = incoming;
                    tCall.phone_number = phone_number;
                    tCall.prevRType = prevRType;
                    tCall.prevRID = prevRID;
                    tCall.position = position;
                    tCall.event_type = event_type;

                    conList.Add(tCall);
                }
                position = jsonObj["meta"]["position"].ToString();
                using (var posReader = new StreamWriter(@"\\NAS3\NOE_Docs$\RALIM\Logs\Base\CallPosition", false)) {
                    posReader.Write(position);
                }
            }

            log.WriteLine("id,user_id,made_at,updated_at,duration,name,resource_id,resource_type,"+
                "outcome_id,outcome,missed,incoming,phone_number,prevRType,prevRID,position,event_type");
            foreach (Call tCall in conList) {
                log.WriteLine(tCall.id + ", " + tCall.user_id + ", " + tCall.made_at + ", " + tCall.updated_at + 
                    ", " + tCall.duration + ", " + tCall.name + ", " + tCall.resource_id + ", " + tCall.resource_type + 
                    ", " + tCall.outcome_id + ", " + tCall.outcome + ", " + tCall.missed + ", " + tCall.incoming + ", " 
                    + tCall.phone_number + ", " + tCall.prevRType + ", " + tCall.prevRID + ", " + tCall.position + ", " + 
                    tCall.event_type);
            }
            log.Flush();
            Stopwatch timer = new Stopwatch();

            using (SqlConnection connection = new SqlConnection(connString)) {
                log.WriteLine("writing a total of " + conList.Count + " events");
                Console.WriteLine("writing a total of " + conList.Count + " events");
                timer.Start();

                foreach (Call call in conList) {
                    using (SqlCommand command = new SqlCommand(line, connection)) {
                        command.Parameters.Add("@id", SqlDbType.Int).Value = call.id;
                        command.Parameters.Add("@user_id", SqlDbType.Int).Value = call.user_id;
                        command.Parameters.Add("@name", SqlDbType.NVarChar).Value = call.name;
                        command.Parameters.Add("@outcome_id", SqlDbType.Int).Value = call.outcome_id;
                        command.Parameters.Add("@outcome", SqlDbType.NVarChar).Value = call.outcome;
                        command.Parameters.Add("@duration", SqlDbType.Int).Value = call.duration;
                        command.Parameters.Add("@phone_number", SqlDbType.NVarChar).Value = call.phone_number;
                        command.Parameters.Add("@incoming", SqlDbType.Bit).Value = call.incoming;
                        command.Parameters.Add("@missed", SqlDbType.Bit).Value = call.missed;
                        command.Parameters.Add("@updated_at", SqlDbType.DateTime).Value = call.updated_at;
                        command.Parameters.Add("@made_at", SqlDbType.DateTime).Value = call.made_at;
                        command.Parameters.Add("@resource_id", SqlDbType.Int).Value = call.resource_id;
                        command.Parameters.Add("@resource_type", SqlDbType.NVarChar).Value = call.resource_type;
                        command.Parameters.Add("@prevRType", SqlDbType.NVarChar).Value = call.prevRType;
                        command.Parameters.Add("@prevRID", SqlDbType.Int).Value = call.prevRID;
                        command.Parameters.Add("@event_type", SqlDbType.NVarChar).Value = call.event_type;
                        command.Parameters.Add("@position", SqlDbType.NVarChar).Value = call.position;

                        try {
                            connection.Open();

                            int result = command.ExecuteNonQuery();

                            if (result < 0) {
                                log.WriteLine("INSERT failed for " + command.ToString());
                                log.Flush();
                                Console.WriteLine("INSERT failed for " + command.ToString());
                            }
                        }
                        catch (Exception ex) {
                            log.WriteLine(ex);
                            log.Flush();
                            Console.WriteLine(ex);
                        }
                        finally {
                            connection.Close();
                        }
                    }
                }
            }
            timer.Stop();
            log.WriteLine("Done at " + DateTime.Now + ", with a write time of " + timer.Elapsed);
            log.Flush();
            Console.WriteLine("Done at " + DateTime.Now + ", with a write time of " + timer.Elapsed);
            
        }

        public static string Get(string url, string token) {
            string body = "";
            try {
                HttpResponse<string> jsonReponse = Unirest.get(url)
                    .header("accept", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .asJson<string>();
                body = jsonReponse.Body.ToString();
                return body;
            }
            catch (Exception ex) {
                log.WriteLine(ex);
                log.Flush();
                Console.WriteLine(ex);
                return body;
            }
        }

        public static DateTime GetLastMonday(DateTime dt) {
            if (dt.DayOfWeek == DayOfWeek.Monday) {
                return dt.AddDays(-7).Date;
            }
            bool stop = false;
            DateTime temp = dt.AddDays(-1);
            while (!stop) {
                if (temp.DayOfWeek == DayOfWeek.Monday) {
                    stop = true;
                }
                else {
                    temp = temp.AddDays(-1);
                }
            }
            return temp.Date;
        }

        public static DateTime GetLastDate() {
            DateTime limit = new DateTime();
            using (SqlConnection connection = new SqlConnection(connString)) {
                string sqlStr = "SELECT MAX([made_at]) FROM [CAMSRALFG].[dbo].[BaseCalls];";
                using (SqlCommand command = new SqlCommand(sqlStr, connection)) {

                    try {
                        connection.Open();

                        SqlDataReader result = command.ExecuteReader();

                        while (result.Read()) {
                            if (!result.IsDBNull(0)) {
                                limit = result.GetDateTime(0);
                            }
                        }

                        if (limit == DateTime.MinValue) {
                            return DateTime.Now.Date.AddDays(-7);
                        }
                        else log.WriteLine("Found max date of " + limit);
                    }
                    catch (Exception ex) {
                        log.WriteLine(ex);
                        log.Flush();
                        Console.WriteLine(ex);
                    }
                    finally {
                        connection.Close();
                    }
                }

            }
            return limit;
        }


        public static void SetOwnerData() {
            string testJSON = Get(@"https://api.getbase.com/v2/users?per_page=100&sort_by=created_at&status=active", token);
            JObject jObj = JObject.Parse(testJSON) as JObject;
            JArray jArr = jObj["items"] as JArray;
            foreach (var obj in jArr) {
                var data = obj["data"];

                if (data["group"].HasValues == false || Convert.ToInt32(data["group"]["id"]) != 84227) {
                    continue; //do not count agents not in sales group stats.
                }

                int tID = Convert.ToInt32(data["id"]);
                string tName = data["name"].ToString();
                ownersData.Add(tID, tName);
            }
        }

        public static void SetOutcomeData() {
            string testJSON = Get(@"https://api.getbase.com/v2/call_outcomes?per_page=100", token);
            JObject jObj = JObject.Parse(testJSON) as JObject;
            JArray jArr = jObj["items"] as JArray;
            outcomeData.Add(0, "Unknown");

            foreach (var obj in jArr) {
                var data = obj["data"];
                int tID = Convert.ToInt32(data["id"]);
                string tName = data["name"].ToString();
                outcomeData.Add(tID, tName);
            }
        }

        public static string RandomString(int length) {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}
