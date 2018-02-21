using System;

namespace BaseCallTracker {
    public class Owner {
        public int id { get; set; }
        public string name { get; set; }
        public int count { get; set; }
        public DateTime first { get; set; }
        public DateTime last { get; set; }

        public Owner(int i, string n, int c, DateTime f, DateTime l) {
            id = i;
            name = n;
            count = c;
            first = f;
            last = l;
        }
    }
}