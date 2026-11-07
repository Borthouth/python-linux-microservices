db = db.getSiblingDB('app_analytics');
const events = [
  {
    event_id: "evt_1001",
    user_email: "alice@example.com",
    customer_id: "CUST-001",
    event_type: "login",
    platform: "web",
    metadata: { browser: "Chrome", ip: "192.168.1.10" },
    timestamp: new Date("2026-03-01T10:15:00Z")
  },
  {
    event_id: "evt_1002",
    user_email: "alice@example.com",
    customer_id: "CUST-001",
    event_type: "feature_used",
    platform: "web",
    metadata: { feature_name: "export_report" },
    timestamp: new Date("2026-03-01T10:18:30Z")
  },
  {
    event_id: "evt_1003",
    user_email: "bob@example.com",
    customer_id: "CUST-002",
    event_type: "login",
    platform: "mobile",
    metadata: { device: "Google Pixel 10a" },
    timestamp: new Date("2026-03-02T14:20:00Z")
  }
];
db.event_logs.bulkWrite(events.map(event => ({
  updateOne: {
    filter: { event_id: event.event_id },
    update: { $setOnInsert: event },
    upsert: true
  }
})));
db.event_logs.createIndex({ user_email: 1 });
db.event_logs.createIndex({ customer_id: 1 });
