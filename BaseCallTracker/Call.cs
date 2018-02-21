using System;

namespace BaseCallTracker {
    public class Call {
        public int id { get; set; }
        public int user_id { get; set; }
        public DateTime made_at { get; set; }
        public DateTime updated_at { get; set; }
        public string name { get; set; }
        public int resource_id { get; set; }
        public string resource_type { get; set; }
        public string outcome { get; set; }
        public int duration { get; set; }
        public bool incoming { get; set; }
        public bool missed { get; set; }
        public int outcome_id { get; set; }
        public string outcome_type { get; set; }
        public string phone_number { get; set; }
        public int external_id { get; set; }
        public int[] associated_deal_ids { get; set; }
        public string summary { get; set; }
        public string prevRType { get; set; }
        public int prevRID { get; set; }
        public string position { get; set; }
        public string event_type { get; set; }

        public Call(int id, int user_id, DateTime made_at, DateTime updated_at, string name, string resource_type,
            string outcome_type, string outcome, int duration, bool incoming, bool missed) {
            this.id = id;
            this.user_id = user_id;
            this.made_at = made_at;
            this.updated_at = updated_at;
            this.name = name;
            this.resource_type = resource_type;
            this.outcome_type = outcome_type;
            this.outcome = outcome;
            this.duration = duration;
            this.incoming = incoming;
            this.missed = missed;
        }

        public Call() { }
    }
}