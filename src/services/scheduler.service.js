const schedule = require("node-schedule");
const tableDefinitions = require("../definitions/tables");
const preactorDefinitions = require("../definitions/preactor"); // Import preactor defs
const syncService = require("./sync.service");

function startScheduler() {
  console.log("[SCHEDULER] Initializing schedules (Timezone: Europe/Sofia)...");

  // ==========================================
  // JOB 1: СУТРЕШЕН (05:00) - ВСИЧКИ СТАНДАРТНИ ТАБЛИЦИ
  // ==========================================
  const ruleMorning = new schedule.RecurrenceRule();
  ruleMorning.hour = 5;
  ruleMorning.minute = 0;
  ruleMorning.tz = "Europe/Sofia";

  schedule.scheduleJob(ruleMorning, async function () {
    console.log(
      `[SCHEDULER - MORNING] Starting FULL Sync at ${new Date().toLocaleString("bg-BG")}`,
    );
    const tablesToSync = Object.keys(tableDefinitions);
    await runBatch(tablesToSync, "MORNING");
  });

  // ==========================================
  // JOB 2: ОБЕДЕН (12:00) - БЕЗ tcibd001
  // ==========================================
  const ruleNoon = new schedule.RecurrenceRule();
  ruleNoon.hour = 12;
  ruleNoon.minute = 0;
  ruleNoon.tz = "Europe/Sofia";

  schedule.scheduleJob(ruleNoon, async function () {
    console.log(
      `[SCHEDULER - NOON] Starting PARTIAL Sync at ${new Date().toLocaleString("bg-BG")}`,
    );
    const tablesToSync = Object.keys(tableDefinitions).filter(
      (key) => key !== "tcibd001",
    );
    await runBatch(tablesToSync, "NOON");
  });

  // ==========================================
  // NEW JOB: PREACTOR QUERIES (03:00)
  // ==========================================
  const rulePreactor = new schedule.RecurrenceRule();
  rulePreactor.hour = 3;
  rulePreactor.minute = 0;
  rulePreactor.tz = "Europe/Sofia";

  schedule.scheduleJob(rulePreactor, async function () {
    console.log(
      `[SCHEDULER - PREACTOR] Starting Preactor Nightly Queries at ${new Date().toLocaleString("bg-BG")}`,
    );

    // Get keys from preactor.js definitions
    const preactorTables = Object.keys(preactorDefinitions);

    await runBatch(preactorTables, "PREACTOR-NIGHT");
  });

  console.log(
    "[SCHEDULER] Jobs scheduled: 03:00 (Preactor), 05:00 (Full Standard) & 12:00 (Partial Standard).",
  );
}

// Помощна функция за въртене на цикъла
async function runBatch(keys, jobName) {
  for (const key of keys) {
    try {
      console.log(`[${jobName}] Auto-syncing: ${key}...`);
      const result = await syncService.syncTable(key);
      console.log(`[${jobName}] SUCCESS: ${key} (${result.rows} rows)`);
    } catch (err) {
      console.error(`[${jobName}] FAILED: ${key} - ${err.message}`);
    }
  }
  console.log(
    `[${jobName}] Batch Job Finished at ${new Date().toLocaleString("bg-BG")}`,
  );
}

module.exports = { startScheduler };
